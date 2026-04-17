#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/epoll.h>

#define MAX_EVENTS 3 // We only need handle two diffrent client connection events, 
// so 3 is enough for us (one for server socket, two for client sockets) for this part 

void set_non_blocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0); // get current flags
  if (flags == -1) {
    std::cerr << "Failed to get file descriptor flags\n";
    return;
  }
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    std::cerr << "Failed to set non-blocking mode\n";
  }
}
int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  // -- EVENT_LOOP --

  set_non_blocking(server_fd);

  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    std::cerr << "Failed to create epoll instance\n";
    return 1;
  }

  epoll_event ev{};
  ev.events = EPOLLIN;
  ev.data.fd = server_fd;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
    std::cerr << "Failed to add server socket to epoll\n";
    return 1;
  }

  epoll_event events[MAX_EVENTS];

  const char *response = "+PONG\r\n";
  char buffer[1024];

  std::cout << "Waiting for a client to connect...\n";

  while (true) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);

    for (int i = 0; i < nfds; ++i) {
      int fd = events[i].data.fd;

      if (fd == server_fd) {
        // New client connection
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
        if (client_fd < 0) {
          std::cerr << "Failed to accept new client\n";
          continue;
        }
        set_non_blocking(client_fd);

        ev.events = EPOLLIN | EPOLLET; // Edge-triggered
        ev.data.fd = client_fd;

        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) == -1) {
          std::cerr << "Failed to add client socket to epoll\n";
          close(client_fd);
          continue;
        }
        std::cout << "Client connected: " << inet_ntoa(client_addr.sin_addr) << "\n";
      } else {
        // Data from a client
        ssize_t count = read(fd, buffer, sizeof(buffer) - 1);
        if (count <= 0) {
          // Client disconnected or error
          std::cerr << "Client disconnected or read error\n";
          close(fd);
          continue;
        }
        buffer[count] = '\0'; // Null-terminate the buffer

        std::cout << "Received from client: " << buffer;

        // Respond with PONG
        if (write(fd, response, strlen(response)) < 0) {
          std::cerr << "Failed to write response to client\n";
          close(fd);
          continue;
        }
      }
    }
  }
  /*
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  int clinet_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
  std::cout << "Client connected\n";

  while (true) {
    //keep the main thread alive to accept new clients
    int new_client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    if (new_client_fd <= 0) {
      std::cerr << "Failed to accept new client\n";
      close(new_client_fd);
      continue;
    }

    int *pclinet = new int;
    *pclinet = new_client_fd;
    pthread_t thread_id;

    if (pthread_create(&thread_id, nullptr, handle_client, pclinet) != 0) {
      std::cerr << "Failed to create thread for new client\n";
      delete pclinet;
      continue;
    }
  }

  */
  return 0;
}
