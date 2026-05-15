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

```
Client connects
     │
     ▼
epoll_wait() ──► new data ──► RespParser::append()
                                     │
                              getCommand() ready?
                                     │
                                     ▼
                              dispatch() ──► Command::execute()
                                                    │
                                                    ▼
                                          conn.output_buffer += response
                                                    │
                                                    ▼
                                          flush_output() ──► write() to socket
```

---

## Implemented commands

### Core
| Command | Notes |
|---|---|
| `PING` | with optional message |
| `ECHO` | |
| `SET` | with `EX` and `PX` expiry options |
| `GET` | with lazy expiry deletion |

### Lists
| Command | Notes |
|---|---|
| `RPUSH` | appends elements, wakes blocked `BLPOP` clients |
| `LPUSH` | prepends elements, correct Redis ordering (`LPUSH key a b c` → `[c, b, a]`) |
| `LRANGE` | with negative index support |
| `LPOP` | with optional `count` argument |
| `LLEN` | |
| `BLPOP` | blocking pop with fractional second timeout (e.g. `0.2`) |

### Streams
| Command | Notes |
|---|---|
| `XADD` | auto ID (`*`), partial auto ID (`ms-*`), full ID validation |
| `XRANGE` | with `-` and `+` boundary support |
| `XREAD` | with `STREAMS`, `COUNT`, `BLOCK` — including `$` for new-entries-only |

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
- Cleaning up `XAddCommand` (ID validation edge cases, `0-0` check, wake-up logic for blocked `XREAD`)
- Finalising `XReadCommand` with full `BLOCK` support, `$` ID resolution, and integration with `build_xread_response()`
- Unifying timeout handling in `check_blpop_timeouts()` to cover both `BLPOP` and `XREAD BLOCK` in one pass
- General cleanup: removing duplicated ID-parsing logic, standardising error responses

---

## Building and running

```bash
# Build
g++ -std=c++23 -O2 -o server server.cpp

# Run (listens on port 6379)
./server

# Test with redis-cli
redis-cli PING
redis-cli SET foo bar EX 5
redis-cli RPUSH mylist a b c
redis-cli BLPOP mylist 1
redis-cli XADD mystream '*' field value
redis-cli XREAD STREAMS mystream 0-0
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
