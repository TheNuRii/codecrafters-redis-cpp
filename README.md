[![progress-banner](https://backend.codecrafters.io/progress/redis/b779a5e1-f8bd-43ab-8a9c-0b1feb7abb28)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

# codecrafters-redis-cpp

A from-scratch Redis server implementation in modern C++, built as part of the [CodeCrafters](https://codecrafters.io) "Build Your Own Redis" challenge. The goal is to deeply understand how Redis works internally — event loops, RESP protocol, data structures, blocking commands, and streams — by implementing it all by hand, without any Redis libraries.

---

## What this is

This project is both a **learning exercise** and a **portfolio piece**. Rather than using Redis as a black box, I'm building the core of it from the ground up: a TCP server that speaks the Redis Serialization Protocol (RESP), handles multiple concurrent clients via `epoll`, and supports a growing subset of Redis commands.

---

## Architecture

The server is single-threaded and uses **Linux epoll in edge-triggered mode** for non-blocking I/O — the same fundamental model Redis itself uses. Each client connection is tracked with a `Connection` struct that holds per-client read/write buffers, blocking state, and expiry deadlines.

Commands are implemented as a class hierarchy (`Command` base class, one subclass per command), registered in a dispatch table at startup. The RESP parser is incremental — it handles partial reads correctly by buffering input until a full command is available.

---

## Architecture

```
clients (TCP)
     │
     ▼
┌─────────────┐
│ epoll loop  │  edge-triggered, single thread
│ (server.cpp)│
└──────┬──────┘
       │  raw bytes
       ▼
┌─────────────┐
│ RespParser  │  stateful, handles partial TCP frames
└──────┬──────┘
       │  vector<string>
       ▼
┌──────────────────┐
│ CommandRegistry  │  O(1) dispatch via unordered_map
└──────┬───────────┘
       │
       ▼
┌──────────────────────────────┐
│           DataStore          │
│  ┌─────────┐  ┌───────────┐ │
│  │ KVStore │  │ ListStore │ │
│  └─────────┘  └───────────┘ │
│  ┌─────────────────────────┐ │
│  │      StreamStore        │ │
│  └─────────────────────────┘ │
└──────────────────────────────┘
```

### Key design decisions

**Single thread, epoll edge-triggered** — no locking required anywhere. Edge-triggered mode means the kernel notifies once per state change, requiring a full drain loop on every `EPOLLIN` event. More complex to implement, but avoids spurious wakeups.

**RESP parser is stateful** — TCP is a stream protocol. A single command may arrive split across multiple `read()` calls. `RespParser` buffers partial input and only yields a command when fully received.

**Blocking commands without blocking the server** — `BLPOP` and `XREAD BLOCK` register the client's file descriptor in a wait list inside `DataStore`. The event loop continues handling other clients normally. When `RPUSH` or `XADD` arrives, it wakes up registered waiters by writing directly to their output buffers. Timeouts are handled via a dynamic `epoll_wait` timeout computed from the nearest deadline.

**RAII socket ownership** — file descriptors are wrapped in a `Socket` class. `close()` is called exactly once, in the destructor, guaranteed even on early returns.

---

## Supported commands

| Command | Notes |
|---|---|
| `PING [message]` | |
| `ECHO message` | |
| `SET key value [EX seconds] [PX ms]` | lazy expiry |
| `GET key` | |
| `TYPE key` | returns `string`, `list`, `stream`, or `none` |
| `LPUSH key value [value ...]` | |
| `RPUSH key value [value ...]` | unblocks `BLPOP` waiters |
| `LPOP key [count]` | |
| `LRANGE key start stop` | negative indices supported |
| `LLEN key` | |
| `BLPOP key timeout` | blocks until data or timeout |
| `XADD key id field value [...]` | `*`, `ms-*`, and explicit IDs |
| `XRANGE key start end` | `-` and `+` supported |
| `XREAD [COUNT n] [BLOCK ms] STREAMS key id` | `$` for new-entries-only |

---

## Key implementation details

**Expiry** is implemented via lazy deletion — keys are not actively removed on a timer, but checked on every `GET`. This avoids background threads and race conditions entirely.

**BLPOP blocking** works by registering the connection in a waiter list keyed by the list name. When `RPUSH` adds elements, it immediately wakes any waiting connections by writing to their `output_buffer`. The `epoll_wait` timeout is computed dynamically based on the nearest `BLPOP` deadline, so timeouts fire accurately without polling.

**XREAD BLOCK** follows the same pattern as `BLPOP` — blocked connections are registered in `xread_waiters`. When `XADD` inserts a new entry, it wakes all connections waiting on that stream key.

**RESP parsing** is done with a stateful `RespParser` class per connection. It handles fragmented TCP reads correctly — a command is only dispatched once all bytes have arrived.

---

## Current status

Core commands, lists, expiry, and streams are working and passing CodeCrafters tests.

**Currently in progress — active refactoring:**

The project started as a working but monolithic implementation and is currently being restructured into the clean architecture described above. The refactor covers safety (RAII sockets, eliminating raw Connection* pointers in favour of stable file descriptors), correctness (enum-based type system fixing silent type corruption bugs, unified BlockState making invalid connection states unrepresentable), architecture (decomposing a God Object DataStore into focused sub-stores, replacing a global command map with an encapsulated CommandRegistry), and performance (switching vector to deque for O(1) front pop, eliminating hot-path argument copies with std::span). Modularization into the file structure shown above and this documentation are the final remaining steps.

---

## Build & run

**Requirements:** GCC 12+ or Clang 15+, CMake 3.20+, Linux (epoll).

```bash
git clone https://github.com/<you>/redis-clone
cd redis-clone
mkdir build && cd build

# debug build — AddressSanitizer + UBSan enabled
cmake .. -DCMAKE_BUILD_TYPE=Debug
cmake --build .

./redis_clone
```

Connect with any Redis client:

```bash
redis-cli ping
redis-cli set foo bar
redis-cli get foo
```

---

## What I learned

- How `epoll` edge-triggered mode works and why you must drain the read buffer completely on each event
- Why Redis uses lazy expiry rather than active background deletion
- The subtleties of RESP framing — partial reads, inline vs bulk strings, null bulk vs null array
- How blocking commands (`BLPOP`, `XREAD BLOCK`) are implemented without threads — purely through event-driven wake-up
- Redis stream ID semantics — monotonicity guarantees, partial auto-generation (`ms-*`), and the `$` cursor

---

## References

- [Redis Serialization Protocol (RESP)](https://redis.io/docs/reference/protocol-spec/)
- [Redis source code](https://github.com/redis/redis)
- [CodeCrafters — Build Your Own Redis](https://codecrafters.io/challenges/redis)
