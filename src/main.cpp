#include <istream>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <cstring>
#include <errno.h>
#include <algorithm>
#include <memory>
#include <iterator>
#include <chrono>

#define MAX_EVENTS 64
#define BUFFER_SIZE 1024

struct Connection {
  int fd;
  std::string input_buffer;
  std::string output_buffer;

  bool blocked = false;
  std::string blocking_key;
};

class DataStore {
public:
  std::unordered_map<std::string, std::string> kv;
  std::unordered_map<std::string, std::vector<std::string>> lists;
  std::unordered_map<std::string, std::vector<Connection*>> blocked;
  std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiry;
};

// ----------------- COMMAND parsing and execution

std::string bulk(const std::string& s) {
  return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

std::string null_bulk() {
  return "$-1\r\n"; 
}

std::string integer_resp(int n) {
  return ":" + std::to_string(n) + "\r\n";
}

std::string array(const std::vector<std::string>& v) {
  std::string res = "*" + std::to_string(v.size()) + "\r\n";

  for (auto& s : v) {
    res += bulk(s);
  }

  return res;
}

class RespParser {
public:
    void append(const std::string& data) {
        buffer += data;
    }

    bool getCommand(std::vector<std::string>& out) {
        if (buffer.empty() || buffer[0] != '*') return false;

        size_t pos = 1;
        size_t end = buffer.find("\r\n", pos);
        if (end == std::string::npos) return false;

        int count = std::stoi(buffer.substr(pos, end - pos));
        pos = end + 2;

        std::vector<std::string> result;

        for (int i = 0; i < count; i++) {
            if (pos >= buffer.size() || buffer[pos] != '$') return false;

            size_t len_end = buffer.find("\r\n", pos);
            if (len_end == std::string::npos) return false;

            int len = std::stoi(buffer.substr(pos + 1, len_end - pos - 1));
            pos = len_end + 2;

            if (pos + len + 2> buffer.size()) return false;

            result.push_back(buffer.substr(pos, len));
            pos += len + 2;
        }

        buffer.erase(0, pos);
        out = result;
        return true;
    }

private:
    std::string buffer;
}; 

class Command {
public:
    virtual std::string execute(DataStore&, Connection&, const std::vector<std::string>&) = 0;
    virtual ~Command() = default;
};

class PingCommand : public Command {
public:
    std::string execute(DataStore&, Connection&, const std::vector<std::string>& args) override {
        if (args.empty()) {
            return "+PONG\r\n";
        }
        return "+" + args[0] + "\r\n";
    }
};

class EchoCommand : public Command {
public:
    std::string execute(DataStore&, Connection&, const std::vector<std::string>& args) override {
        if (args.empty()) return null_bulk();
        return bulk(args[0]);
    }
};

class SetCommand : public Command {
public:
  std::string execute(DataStore& store, Connection&, const std::vector<std::string>& args) override {
    if (args.size() < 2) return "-ERR wrong number of arguments\r\n";

    store.kv[args[0]] = args[1];
    store.expiry.erase(args[0]); // clear exp if was present

    // Find option EX / PX (case-insensitive)
    for (size_t i = 2; i + 1 < args.size(); i++) {
      std::string opt = args[i];
      std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);

      if (opt == "EX") {
        int seconds = std::stoi(args[i + 1]);
        store.expiry[args[0]] = std::chrono::steady_clock::now()
                              + std::chrono::seconds(seconds);
        break;
      } else if (opt == "PX") {
        int ms = std::stoi(args[i + 1]);
        store.expiry[args[0]] = std::chrono::steady_clock::now()
                              + std::chrono::milliseconds(ms);
        break;
      }
    }

    return "+OK\r\n";
  }
};

class GetCommand : public Command {
public:
  std::string execute(DataStore& store, Connection&, const std::vector<std::string>& args) override {
    if (args.empty()) return "-ERR wrong number of arguments\r\n";

    const std::string& key = args[0];

    // Lazy expiry check
    auto exp_it = store.expiry.find(key);
    if (exp_it != store.expiry.end()) {
      if (std::chrono::steady_clock::now() >= exp_it->second) {
        store.kv.erase(key);
        store.expiry.erase(exp_it);
        return null_bulk();
      }
    }

    auto it = store.kv.find(key);
    if (it == store.kv.end()) return null_bulk();
    return bulk(it->second);
  }
};

class LLenCommand : public Command {
public:
  std::string execute(DataStore& store, Connection&, const std::vector<std::string>& args) override {
    if (args.empty()) return "-ERR wrong args\r\n";

    if (!store.lists.count(args[0])) return integer_resp(0);
    const auto& list = store.lists[args[0]];

    return ":" + std::to_string(list.size()) + "\r\n";
  }
};

class LPushCommand : public Command {
public:
  std::string execute(DataStore& store, Connection&, const std::vector<std::string>& args) override {
    if (args.size() < 2) return "-ERR wrong args\r\n";

    auto& list = store.lists[args[0]];
    for (size_t i = 1; i < args.size(); i++) {
      list.insert(list.begin(), args[i]);
    }

    return integer_resp(list.size());
  }
};

class RPushCommand : public Command {
public:
  std::string execute(DataStore& store, Connection&, const std::vector<std::string>& args) override {
    if (args.size() < 2) return "-ERR wrong number of arguments\r\n";
    const std::string& key = args[0];
    auto& list = store.lists[key];
    for (size_t i = 1; i < args.size(); i++)
      list.push_back(args[i]);

    
    int size_after_insert = (int)list.size();

    if (store.blocked.count(key)) {
      auto& waiters = store.blocked[key];

      while (!waiters.empty() && !list.empty()) {
        Connection* c = waiters.front();
        waiters.erase(waiters.begin());
        std::string val = list.front();
        list.erase(list.begin());
        c->output_buffer += array({key, val});
        c->blocked = false;
      }

      if (waiters.empty()) store.blocked.erase(key);
    }

    return integer_resp(size_after_insert);
  }
};

class LRangeCommand : public Command {
public:
  std::string execute(DataStore& store, Connection&, const std::vector<std::string>& args) override {
    if (args.size() < 3) return "-ERR wrong args\r\n";
    if (!store.lists.count(args[0])) return "*0\r\n";

    const auto& list = store.lists[args[0]];
    int start = std::stoi(args[1]);
    int end = std::stoi(args[2]);

    if (start < 0) start += list.size();
    if (end < 0) end += list.size();

    if (start < 0) start = 0;
    if (end >= list.size()) end = list.size() - 1;

    if (start > end) return "*0\r\n";

    return array(std::vector<std::string>(list.begin() + start, list.begin() + end + 1));
  }
};

class LPopCommand : public Command {
public:
  std::string execute(DataStore& store, Connection&, const std::vector<std::string>& args) override {
    if (args.empty()) return "-ERR wrong number of arguments\r\n";

    auto it = store.lists.find(args[0]);
    if (it == store.lists.end() || it->second.empty()) return null_bulk();
    auto& list = it->second;

    int count = 1;
    if (args.size() >= 2) count = std::stoi(args[1]);
    bool return_array = (args.size() >= 2);

    count = std::min(count, (int)list.size()); 

    std::vector<std::string> popped;
    for (int i = 0; i < count; i++) {
      popped.push_back(list.front());
      list.erase(list.begin());
    }

    if (!return_array) return bulk(popped[0]); 
    return array(popped);                       
  }
};

class BLPopCommand : public Command {
public:
  std::string execute(DataStore& store, Connection& conn, const std::vector<std::string>& args) override {
    if (args.size() < 2) return "-ERR wrong number of arguments\r\n";
    
    const std::string& key = args[0];
    auto it = store.lists.find(key);

    if (it != store.lists.end() && !it->second.empty()) {
      std::string val = it->second.front();
      it->second.erase(it->second.begin());
      
      return array({key, val});
    }

    // RPush will unblok it latter
    conn.blocked = true;
    conn.blocking_key = key;
    store.blocked[key].push_back(&conn);
    return ""; // send nothing now
  }
};

std::unordered_map<std::string, std::unique_ptr<Command>> commands;

void init_commands() {
  commands["PING"]   = std::make_unique<PingCommand>();
  commands["ECHO"]   = std::make_unique<EchoCommand>();
  commands["SET"]    = std::make_unique<SetCommand>();
  commands["GET"]    = std::make_unique<GetCommand>();

  commands["LLEN"]   = std::make_unique<LLenCommand>();
  commands["LPUSH"]  = std::make_unique<LPushCommand>();
  commands["RPUSH"]  = std::make_unique<RPushCommand>();
  commands["LRANGE"] = std::make_unique<LRangeCommand>();
  commands["LPOP"]   = std::make_unique<LPopCommand>();
  commands["BLPOP"]  = std::make_unique<BLPopCommand>();
}

std::string dispatch(DataStore& store, Connection& conn, std::vector<std::string>& cmd) {
    if (cmd.empty()) return "-ERR\r\n";

    std::string op = cmd[0];
    std::transform(op.begin(), op.end(), op.begin(), ::toupper);

    if (!commands.count(op)) return "-ERR unknown\r\n";

    return commands[op]->execute(store, conn, 
        std::vector<std::string>(cmd.begin() + 1, cmd.end()));
}

// ---------------------------------------------------

void set_non_blocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void flush_output(Connection& conn, int epoll_fd) {
  while (!conn.output_buffer.empty()) {
    ssize_t n = write(conn.fd, conn.output_buffer.c_str(), conn.output_buffer.size());

    if (n < 0) {
      if (errno = EAGAIN) {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
        ev.data.fd = conn.fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn.fd, &ev);
      }

      return;
    }

    conn.output_buffer.erase(0, n);
  }

  epoll_event ev{};
  ev.events = EPOLLIN | EPOLLET;
  ev.data.fd = conn.fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn.fd, &ev);
}

int main() {
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);

  int reuse = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(6379);
  addr.sin_addr.s_addr = INADDR_ANY;

  bind(server_fd, (sockaddr*)&addr, sizeof(addr));
  listen(server_fd, SOMAXCONN);

  set_non_blocking(server_fd);

  int epoll_fd = epoll_create1(0);

  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = server_fd;
  epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

  std::unordered_map<int, Connection> clients;
  std::unordered_map<int, RespParser> parsers;
  DataStore store;

  init_commands();

  epoll_event events[MAX_EVENTS];

  while (true) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);

    for (int i = 0; i < nfds; i++) {
      int fd = events[i].data.fd;
      uint32_t ev_flags = events[i].events;

      if (fd == server_fd) {
        while (true) {
          int client_fd = accept(server_fd, nullptr, nullptr);
          if (client_fd < 0) break;

          set_non_blocking(client_fd);

          epoll_event cev{};
          cev.events = EPOLLIN | EPOLLET;
          cev.data.fd = client_fd;
          epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &cev);

          clients[client_fd] = Connection{client_fd};
          parsers[client_fd] = RespParser();
        }

        continue;
      }

      if (ev_flags & EPOLLOUT) {
        flush_output(clients[fd], epoll_fd);
      }

      if (ev_flags & EPOLLIN) {
        auto& conn = clients[fd];
        auto& parser = parsers[fd];
        bool close_conn = false;

        while (true) {
          char buf[BUFFER_SIZE];
          ssize_t n = read(fd, buf, sizeof(buf));

          if (n == 0) {
            close_conn = true;
            break;
          }

          if (n < 0) {
            if (errno != EAGAIN) close_conn = true;
            break;
          }

          parser.append(std::string(buf, n));
        }

        if (close_conn) {
          if (conn.blocked) {
            auto& waiters = store.blocked[conn.blocking_key];
            waiters.erase(std::remove(waiters.begin(), waiters.end(), &conn), waiters.end());
          }

          close(fd);
          clients.erase(fd);
          parsers.erase(fd);
          continue;
        }

        if (conn.blocked) continue; // dont process commands while blocked

        std::vector<std::string> cmd;
        while (!conn.blocked && parser.getCommand(cmd)) {
          std::string resp = dispatch(store, conn, cmd);

          if (!resp.empty()) conn.output_buffer += resp;
        }

        if (!conn.output_buffer.empty()) {
          flush_output(conn, epoll_fd);
        }

        // Flush buffer of any cliensts unblocked by RPush
        for (auto& [cdf, c] : clients) {
          if (!c.blocked && !c.output_buffer.empty()) {
            flush_output(c, epoll_fd);
          }
        }
      }
    }
  }
}