// =============================================================================
//  Multi-Tier Key-Value Store
//
//  Architecture (request path):
//
//     client ── TCP ──> accept loop ──> bounded queue ──> worker thread
//                                                              │
//                                              ┌───────────────┴──────────────┐
//                                              │                              │
//                                        sharded cache                  group-commit WAL
//                                       (CLOCK eviction)                       │
//                                              │                        PostgreSQL
//                                              └──────── on miss ─────────────┘
//
//  Four ideas worth being able to defend in an interview:
//
//   1. SHARDED CACHE.  One global lock on a cache makes the cache itself the
//      contention point.  We split the keyspace across N independent shards,
//      each with its own lock.  Threads touching different keys usually touch
//      different shards and never contend.
//
//   2. CLOCK EVICTION.  Classic LRU must move a node to the head of a list on
//      every hit -- so a "read" is structurally a write, and a reader-writer
//      lock buys nothing.  CLOCK approximates LRU using one reference bit per
//      slot.  A hit only sets that bit, which we make atomic, so the read path
//      is genuinely read-only and can run under a shared lock.
//
//   3. GROUP-COMMIT WAL.  fdatasync() is expensive (~0.1-10ms).  Paying it per
//      write caps throughput at 1/fsync_latency.  Instead a single writer
//      thread accumulates records and flushes when either the batch is full OR
//      a time window expires -- so throughput scales under load without letting
//      latency drift when traffic is light.  This is how real databases do it.
//
//   4. BACKPRESSURE.  Every queue here is bounded (accept queue, WAL queue).
//      An unbounded queue under overload does not fail gracefully -- it grows
//      until the OOM killer intervenes.  Bounded queues push back instead.
//
//  Build:
//     g++ -std=c++17 -O2 -pthread server.cpp -lpqxx -lpq -o server
//
//  Run:
//     ./server "host=127.0.0.1 port=5432 dbname=kvstore user=$(whoami)"
//
//  API:
//     PUT    /kv/<key>     body = value
//     GET    /kv/<key>
//     DELETE /kv/<key>
//     GET    /metrics
// =============================================================================

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <pqxx/pqxx>

// ----------------------------------------------------------------------------
//  Tunables
// ----------------------------------------------------------------------------
namespace cfg {
constexpr int      PORT              = 8080;
constexpr size_t   NUM_SHARDS        = 16;    // cache shards
constexpr size_t   CACHE_CAPACITY    = 4096;  // total slots across all shards
constexpr size_t   ACCEPT_QUEUE_CAP  = 1024;  // bounded handoff to workers
constexpr size_t   DB_POOL_SIZE      = 8;     // must stay under max_connections
constexpr size_t   WAL_BATCH         = 64;    // flush when this many records queued
constexpr auto     WAL_WINDOW        = std::chrono::milliseconds(5);
constexpr size_t   WAL_MAX_PENDING   = 4096;  // backpressure threshold
constexpr size_t   MAX_HEADER_BYTES  = 64 * 1024;
constexpr size_t   MAX_BODY_BYTES    = 8 * 1024 * 1024;
constexpr int      KEEPALIVE_MAX_REQ = 100;   // requests per connection
}  // namespace cfg

// ----------------------------------------------------------------------------
//  Metrics
// ----------------------------------------------------------------------------
struct Metrics {
    std::atomic<uint64_t> gets{0}, puts{0}, deletes{0};
    std::atomic<uint64_t> cache_hits{0}, cache_misses{0};
    std::atomic<uint64_t> db_reads{0}, db_writes{0};
    std::atomic<uint64_t> evictions{0};
    std::atomic<uint64_t> wal_records{0}, wal_flushes{0};
    std::atomic<uint64_t> queue_rejects{0};
};
static Metrics g_metrics;

// ----------------------------------------------------------------------------
//  Portable durable sync.
//
//  Linux has fdatasync(): flush data, skip metadata we do not need.  macOS does
//  not have it at all.  Worse, fsync() on macOS only pushes data to the drive's
//  write cache -- the drive may still lose it on power failure.  F_FULLFSYNC is
//  the call that actually forces a platter/flash commit, which is why it is
//  dramatically slower.  Falling back to plain fsync() when F_FULLFSYNC is
//  unsupported (some filesystems) keeps it working, with weaker guarantees.
// ----------------------------------------------------------------------------
static int durable_sync(int fd) {
#if defined(__APPLE__)
    if (::fcntl(fd, F_FULLFSYNC) != -1) return 0;
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}

// ----------------------------------------------------------------------------
//  Small helper: write() that survives partial writes and EINTR.
//
//  A single write() on a socket is NOT guaranteed to send everything.  Ignoring
//  the return value is one of the most common bugs in hand-rolled servers: it
//  works for small responses on an idle machine and truncates under load.
// ----------------------------------------------------------------------------
static bool write_all(int fd, const char* buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::write(fd, buf + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// =============================================================================
//  1. GROUP-COMMIT WRITE-AHEAD LOG
//
//  Durability contract: append() returns an LSN (log sequence number).  The
//  record is on stable storage once durable_lsn >= that LSN.  Callers that need
//  durability call wait_durable(lsn); callers that don't, skip it.
//
//  The writer thread is the ONLY thread that touches the file descriptor, so
//  there is no locking around the actual I/O -- the lock protects the in-memory
//  buffer only, and is released before the (slow) write+fdatasync.
// =============================================================================
class WalWriter {
public:
    explicit WalWriter(const std::string& path) {
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            throw std::runtime_error("WAL: cannot open " + path);
        }
        writer_ = std::thread(&WalWriter::run, this);
    }

    ~WalWriter() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_writer_.notify_all();
        if (writer_.joinable()) writer_.join();
        if (fd_ >= 0) ::close(fd_);
    }

    // Append a record.  Blocks if too many records are already undurable --
    // that is the backpressure valve.  Returns the record's LSN.
    uint64_t append(const std::string& rec) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_space_.wait(lk, [this] {
            return (next_lsn_ - durable_lsn_) < cfg::WAL_MAX_PENDING || stop_;
        });
        if (stop_) return next_lsn_;

        buffer_ += rec;
        buffer_ += '\n';
        uint64_t lsn = ++next_lsn_;
        ++pending_records_;
        g_metrics.wal_records.fetch_add(1, std::memory_order_relaxed);

        // Always wake the writer -- otherwise a low-traffic workload would park
        // forever waiting for a batch that never fills.  Batching is the
        // writer's job (via its timed window), not the producer's.
        cv_writer_.notify_one();
        return lsn;
    }

    // Block until the given LSN is on stable storage.
    void wait_durable(uint64_t lsn) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_durable_.wait(lk, [this, lsn] { return durable_lsn_ >= lsn || stop_; });
    }

private:
    void run() {
        while (true) {
            std::string batch;
            uint64_t batch_lsn = 0;

            {
                std::unique_lock<std::mutex> lk(mu_);

                // Sleep until there is something to do.
                cv_writer_.wait(lk, [this] { return !buffer_.empty() || stop_; });
                if (stop_ && buffer_.empty()) break;

                // GROUP COMMIT: we now have at least one record.  Rather than
                // flushing it immediately, wait up to WAL_WINDOW for more to
                // arrive so they can share one fdatasync.  Bail out early once
                // the batch is full -- that keeps latency bounded under load
                // while still amortising the fsync when traffic is light.
                if (pending_records_ < cfg::WAL_BATCH && !stop_) {
                    cv_writer_.wait_for(lk, cfg::WAL_WINDOW, [this] {
                        return pending_records_ >= cfg::WAL_BATCH || stop_;
                    });
                }

                batch.swap(buffer_);          // take the whole buffer
                batch_lsn = next_lsn_;        // everything up to here is in `batch`
                pending_records_ = 0;
            }  // lock released BEFORE the expensive I/O

            if (!batch.empty()) {
                if (write_all(fd_, batch.data(), batch.size())) {
                    durable_sync(fd_);        // the expensive part, paid once per batch
                    g_metrics.wal_flushes.fetch_add(1, std::memory_order_relaxed);
                } else {
                    std::cerr << "WAL: write failed\n";
                }
            }

            {
                std::lock_guard<std::mutex> lk(mu_);
                durable_lsn_ = batch_lsn;
            }
            cv_durable_.notify_all();
            cv_space_.notify_all();
        }
        // Wake anyone still parked so shutdown does not hang.
        cv_durable_.notify_all();
        cv_space_.notify_all();
    }

    int fd_{-1};
    std::mutex mu_;
    std::condition_variable cv_writer_;   // producers -> writer
    std::condition_variable cv_durable_;  // writer -> waiters
    std::condition_variable cv_space_;    // writer -> blocked producers
    std::string buffer_;
    uint64_t next_lsn_{0};
    uint64_t durable_lsn_{0};
    size_t pending_records_{0};
    bool stop_{false};
    std::thread writer_;
};

// =============================================================================
//  2. SHARDED CACHE WITH CLOCK EVICTION
//
//  Each shard owns: a fixed slot array, a key -> slot index map, a lock, and an
//  eviction hand.  Shard selection is hash(key) % NUM_SHARDS.
//
//  Why the read path can use a SHARED lock here (and could not with LRU):
//  a cache hit only needs to (a) look up the index, (b) copy the value out, and
//  (c) set a reference bit.  None of those mutate the container's structure.
//  The reference bit is std::atomic<bool>, so concurrent setters are safe even
//  though they hold only a shared lock.
//
//  Note the value is COPIED out under the lock.  Returning a pointer into the
//  slot and unlocking would be a use-after-free: another thread can evict that
//  slot before the caller finishes with the data.
// =============================================================================
class ShardedClockCache {
public:
    ShardedClockCache(size_t num_shards, size_t total_capacity)
        : shards_(num_shards) {
        size_t per_shard = std::max<size_t>(1, total_capacity / num_shards);
        for (auto& s : shards_) s.init(per_shard);
    }

    bool get(const std::string& key, std::string& out) {
        Shard& s = shard_for(key);
        std::shared_lock<std::shared_mutex> lk(s.mu);
        auto it = s.index.find(key);
        if (it == s.index.end()) return false;
        Slot& slot = s.slots[it->second];
        slot.ref.store(true, std::memory_order_relaxed);  // second chance
        out = slot.value;                                  // copy under the lock
        return true;
    }

    void put(const std::string& key, const std::string& value) {
        Shard& s = shard_for(key);
        std::unique_lock<std::shared_mutex> lk(s.mu);

        auto it = s.index.find(key);
        if (it != s.index.end()) {
            Slot& slot = s.slots[it->second];
            slot.value = value;
            slot.ref.store(true, std::memory_order_relaxed);
            return;
        }

        size_t idx = find_victim(s);
        Slot& slot = s.slots[idx];
        if (slot.occupied) {
            s.index.erase(slot.key);
            g_metrics.evictions.fetch_add(1, std::memory_order_relaxed);
        }
        slot.key = key;
        slot.value = value;
        slot.occupied = true;
        slot.ref.store(true, std::memory_order_relaxed);
        s.index[key] = idx;
    }

    void erase(const std::string& key) {
        Shard& s = shard_for(key);
        std::unique_lock<std::shared_mutex> lk(s.mu);
        auto it = s.index.find(key);
        if (it == s.index.end()) return;
        Slot& slot = s.slots[it->second];
        slot.occupied = false;
        slot.key.clear();
        slot.value.clear();
        slot.ref.store(false, std::memory_order_relaxed);
        s.index.erase(it);
    }

private:
    struct Slot {
        std::string key;
        std::string value;
        std::atomic<bool> ref{false};
        bool occupied{false};
    };

    struct Shard {
        std::shared_mutex mu;
        std::vector<Slot> slots;
        std::unordered_map<std::string, size_t> index;
        size_t hand{0};

        void init(size_t cap) {
            slots = std::vector<Slot>(cap);  // Slot is neither copyable nor movable;
            index.reserve(cap * 2);          // vector(n) value-initialises in place.
        }
    };

    // CLOCK: sweep forward.  An empty slot is taken immediately.  A slot whose
    // reference bit is set gets a second chance (bit cleared, hand advances).
    // The first slot found with a clear bit is the victim.  Because every sweep
    // clears bits, the loop terminates in at most 2 passes.
    // Caller must hold the shard's unique lock.
    size_t find_victim(Shard& s) {
        const size_t n = s.slots.size();
        for (size_t scanned = 0; scanned < 2 * n; ++scanned) {
            size_t i = s.hand;
            s.hand = (s.hand + 1) % n;
            Slot& slot = s.slots[i];
            if (!slot.occupied) return i;
            if (slot.ref.load(std::memory_order_relaxed)) {
                slot.ref.store(false, std::memory_order_relaxed);
                continue;
            }
            return i;
        }
        size_t i = s.hand;
        s.hand = (s.hand + 1) % n;
        return i;  // everything was hot; evict wherever the hand landed
    }

    Shard& shard_for(const std::string& key) {
        return shards_[std::hash<std::string>{}(key) % shards_.size()];
    }

    std::vector<Shard> shards_;
};

// =============================================================================
//  3. POSTGRESQL CONNECTION POOL
//
//  Opening a connection per request costs a TCP handshake, authentication and a
//  backend fork on the server -- tens of milliseconds.  A fixed pool amortises
//  that.  The pool is deliberately smaller than the worker count: workers block
//  briefly waiting for a connection instead of every worker holding one, which
//  keeps us well under PostgreSQL's max_connections (default 100).
// =============================================================================
class PgPool {
public:
    PgPool(const std::string& conninfo, size_t size) {
        for (size_t i = 0; i < size; ++i) {
            pool_.push_back(std::make_unique<pqxx::connection>(conninfo));
        }
    }

    // RAII handle: returns the connection to the pool on scope exit, including
    // on the exception path.  Manual release() calls leak connections the first
    // time a query throws.
    class Handle {
    public:
        Handle(PgPool& p, std::unique_ptr<pqxx::connection> c)
            : pool_(p), conn_(std::move(c)) {}
        ~Handle() { if (conn_) pool_.release(std::move(conn_)); }
        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;
        pqxx::connection& operator*() { return *conn_; }
    private:
        PgPool& pool_;
        std::unique_ptr<pqxx::connection> conn_;
    };

    Handle acquire() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [this] { return !pool_.empty(); });
        auto c = std::move(pool_.back());
        pool_.pop_back();
        return Handle(*this, std::move(c));
    }

private:
    void release(std::unique_ptr<pqxx::connection> c) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            pool_.push_back(std::move(c));
        }
        cv_.notify_one();
    }

    friend class Handle;
    std::mutex mu_;
    std::condition_variable cv_;
    std::vector<std::unique_ptr<pqxx::connection>> pool_;
};

// ----------------------------------------------------------------------------
//  Storage: PostgreSQL access, all of it through the pool.
// ----------------------------------------------------------------------------
class Storage {
public:
    explicit Storage(PgPool& pool) : pool_(pool) {}

    void init_schema() {
        auto h = pool_.acquire();
        pqxx::work w(*h);
        w.exec("CREATE TABLE IF NOT EXISTS kvstore ("
               "key TEXT PRIMARY KEY, value TEXT NOT NULL)");
        w.commit();
    }

    bool put(const std::string& key, const std::string& value) {
        try {
            auto h = pool_.acquire();
            pqxx::work w(*h);
            w.exec_params("INSERT INTO kvstore (key, value) VALUES ($1, $2) "
                          "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                          key, value);
            w.commit();
            g_metrics.db_writes.fetch_add(1, std::memory_order_relaxed);
            return true;
        } catch (const std::exception& e) {
            std::cerr << "db put: " << e.what() << "\n";
            return false;
        }
    }

    bool get(const std::string& key, std::string& out) {
        try {
            auto h = pool_.acquire();
            pqxx::work w(*h);
            pqxx::result r = w.exec_params("SELECT value FROM kvstore WHERE key = $1", key);
            g_metrics.db_reads.fetch_add(1, std::memory_order_relaxed);
            if (r.empty()) return false;
            out = r[0][0].as<std::string>();
            return true;
        } catch (const std::exception& e) {
            std::cerr << "db get: " << e.what() << "\n";
            return false;
        }
    }

    bool erase(const std::string& key) {
        try {
            auto h = pool_.acquire();
            pqxx::work w(*h);
            pqxx::result r = w.exec_params("DELETE FROM kvstore WHERE key = $1", key);
            w.commit();
            return r.affected_rows() > 0;
        } catch (const std::exception& e) {
            std::cerr << "db delete: " << e.what() << "\n";
            return false;
        }
    }

private:
    PgPool& pool_;
};

// =============================================================================
//  4. BOUNDED TASK QUEUE
//
//  push() fails rather than blocking the accept loop -- if the acceptor blocked,
//  the kernel's own backlog would fill and clients would see connection resets
//  with no explanation.  Returning false lets us reject explicitly and count it.
// =============================================================================
class BoundedQueue {
public:
    explicit BoundedQueue(size_t cap) : cap_(cap) {}

    bool push(int fd) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (stop_) return false;
            if (q_.size() >= cap_) return false;
            q_.push_back(fd);
        }
        cv_.notify_one();
        return true;
    }

    // Returns -1 when the queue is stopped and drained.
    int pop() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [this] { return !q_.empty() || stop_; });
        if (q_.empty()) return -1;
        int fd = q_.front();
        q_.pop_front();
        return fd;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
    }

    size_t size() {
        std::lock_guard<std::mutex> lk(mu_);
        return q_.size();
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<int> q_;
    size_t cap_;
    bool stop_{false};
};

// ----------------------------------------------------------------------------
//  Minimal HTTP/1.1 handling
// ----------------------------------------------------------------------------
struct Request {
    std::string method;
    std::string path;
    std::string body;
    bool keep_alive{true};
};

static std::string to_lower(std::string s) {
    for (auto& c : s) c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
    return s;
}

// Reads one request. `carry` holds bytes already read from the socket that
// belong to the NEXT request -- with keep-alive, a single read() can straddle a
// request boundary, and dropping those bytes corrupts the following request.
static bool read_request(int fd, std::string& carry, Request& req) {
    size_t hdr_end = std::string::npos;

    while ((hdr_end = carry.find("\r\n\r\n")) == std::string::npos) {
        if (carry.size() > cfg::MAX_HEADER_BYTES) return false;
        char buf[4096];
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;  // peer closed
        carry.append(buf, static_cast<size_t>(n));
    }

    std::string head = carry.substr(0, hdr_end);
    carry.erase(0, hdr_end + 4);

    std::istringstream hs(head);
    std::string line;
    if (!std::getline(hs, line)) return false;
    {
        std::istringstream rl(line);
        std::string version;
        rl >> req.method >> req.path >> version;
        if (req.method.empty() || req.path.empty()) return false;
        req.keep_alive = (version.find("1.1") != std::string::npos);
    }

    size_t content_length = 0;
    while (std::getline(hs, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string name = to_lower(line.substr(0, colon));
        std::string value = line.substr(colon + 1);
        size_t s = value.find_first_not_of(" \t");
        value = (s == std::string::npos) ? "" : value.substr(s);

        if (name == "content-length") {
            content_length = std::strtoul(value.c_str(), nullptr, 10);
            if (content_length > cfg::MAX_BODY_BYTES) return false;
        } else if (name == "connection") {
            std::string v = to_lower(value);
            if (v.find("close") != std::string::npos) req.keep_alive = false;
            else if (v.find("keep-alive") != std::string::npos) req.keep_alive = true;
        }
    }

    while (carry.size() < content_length) {
        char buf[4096];
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        carry.append(buf, static_cast<size_t>(n));
    }
    req.body = carry.substr(0, content_length);
    carry.erase(0, content_length);
    return true;
}

static bool send_response(int fd, int status, const std::string& body,
                          bool keep_alive, const char* ctype = "text/plain") {
    const char* reason = "OK";
    switch (status) {
        case 200: reason = "OK"; break;
        case 201: reason = "Created"; break;
        case 400: reason = "Bad Request"; break;
        case 404: reason = "Not Found"; break;
        case 500: reason = "Internal Server Error"; break;
        case 503: reason = "Service Unavailable"; break;
        default:  reason = "OK"; break;
    }
    std::ostringstream os;
    os << "HTTP/1.1 " << status << " " << reason << "\r\n"
       << "Content-Type: " << ctype << "\r\n"
       << "Content-Length: " << body.size() << "\r\n"
       << "Connection: " << (keep_alive ? "keep-alive" : "close") << "\r\n\r\n"
       << body;
    std::string s = os.str();
    return write_all(fd, s.data(), s.size());
}

// ----------------------------------------------------------------------------
//  Request handling
// ----------------------------------------------------------------------------
struct Server {
    ShardedClockCache& cache;
    Storage& storage;
    WalWriter& wal;
    BoundedQueue& queue;

    static bool parse_kv_path(const std::string& path, std::string& key) {
        static const std::string prefix = "/kv/";
        if (path.rfind(prefix, 0) != 0) return false;
        key = path.substr(prefix.size());
        auto q = key.find('?');
        if (q != std::string::npos) key = key.substr(0, q);
        return !key.empty();
    }

    std::string metrics_body() const {
        std::ostringstream os;
        uint64_t h = g_metrics.cache_hits.load();
        uint64_t m = g_metrics.cache_misses.load();
        double ratio = (h + m) ? (100.0 * static_cast<double>(h) / static_cast<double>(h + m)) : 0.0;
        os << "gets "            << g_metrics.gets.load()          << "\n"
           << "puts "            << g_metrics.puts.load()          << "\n"
           << "deletes "         << g_metrics.deletes.load()       << "\n"
           << "cache_hits "      << h                              << "\n"
           << "cache_misses "    << m                              << "\n"
           << "cache_hit_ratio " << ratio                          << "\n"
           << "evictions "       << g_metrics.evictions.load()     << "\n"
           << "db_reads "        << g_metrics.db_reads.load()      << "\n"
           << "db_writes "       << g_metrics.db_writes.load()     << "\n"
           << "wal_records "     << g_metrics.wal_records.load()   << "\n"
           << "wal_flushes "     << g_metrics.wal_flushes.load()   << "\n"
           << "queue_rejects "   << g_metrics.queue_rejects.load() << "\n";
        // Records per flush is the number that shows group commit working: at 1.0
        // there is no batching; well above 1.0 means fsync cost is being shared.
        uint64_t flushes = g_metrics.wal_flushes.load();
        if (flushes) {
            os << "wal_records_per_flush "
               << (static_cast<double>(g_metrics.wal_records.load()) / static_cast<double>(flushes))
               << "\n";
        }
        return os.str();
    }

    void handle_put(int fd, const Request& req, const std::string& key) {
        g_metrics.puts.fetch_add(1, std::memory_order_relaxed);

        // WAL first -- that is what "write-ahead" means.  If the process dies
        // after this point, the log still describes the intended mutation.
        uint64_t lsn = wal.append(key + "\t" + req.body);
        wal.wait_durable(lsn);

        if (!storage.put(key, req.body)) {
            send_response(fd, 500, "db error\n", req.keep_alive);
            return;
        }
        // Write-through: cache updated only after the durable store succeeds, so
        // the cache can never serve a value the database does not have.
        cache.put(key, req.body);
        send_response(fd, 201, "created\n", req.keep_alive);
    }

    void handle_get(int fd, const Request& req, const std::string& key) {
        g_metrics.gets.fetch_add(1, std::memory_order_relaxed);
        std::string value;
        if (cache.get(key, value)) {
            g_metrics.cache_hits.fetch_add(1, std::memory_order_relaxed);
            send_response(fd, 200, value, req.keep_alive);
            return;
        }
        g_metrics.cache_misses.fetch_add(1, std::memory_order_relaxed);
        if (storage.get(key, value)) {
            cache.put(key, value);  // read-through fill
            send_response(fd, 200, value, req.keep_alive);
        } else {
            send_response(fd, 404, "not found\n", req.keep_alive);
        }
    }

    void handle_delete(int fd, const Request& req, const std::string& key) {
        g_metrics.deletes.fetch_add(1, std::memory_order_relaxed);
        uint64_t lsn = wal.append(key + "\t<DELETE>");
        wal.wait_durable(lsn);
        bool existed = storage.erase(key);
        cache.erase(key);
        send_response(fd, existed ? 200 : 404,
                      existed ? "deleted\n" : "not found\n", req.keep_alive);
    }

    void handle_connection(int fd) {
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        std::string carry;
        for (int served = 0; served < cfg::KEEPALIVE_MAX_REQ; ++served) {
            Request req;
            if (!read_request(fd, carry, req)) break;

            std::string key;
            bool ok = true;
            if (req.path == "/metrics" && req.method == "GET") {
                ok = send_response(fd, 200, metrics_body(), req.keep_alive);
            } else if (!parse_kv_path(req.path, key)) {
                ok = send_response(fd, 404, "unknown path\n", req.keep_alive);
            } else if (req.method == "PUT" || req.method == "POST") {
                handle_put(fd, req, key);
            } else if (req.method == "GET") {
                handle_get(fd, req, key);
            } else if (req.method == "DELETE") {
                handle_delete(fd, req, key);
            } else {
                ok = send_response(fd, 400, "unsupported method\n", req.keep_alive);
            }

            if (!ok || !req.keep_alive) break;
        }
        ::close(fd);
    }

    void worker_loop() {
        while (true) {
            int fd = queue.pop();
            if (fd < 0) break;
            handle_connection(fd);
        }
    }
};

// ----------------------------------------------------------------------------
//  Shutdown plumbing: the self-pipe trick.
//
//  A signal handler may only touch async-signal-safe things, so it cannot take
//  a mutex or notify a condition variable.  Writing one byte to a pipe IS safe,
//  and the accept loop selects on both the listening socket and the pipe -- so
//  a signal interrupts the blocking wait immediately and cleanly.
// ----------------------------------------------------------------------------
static int g_shutdown_pipe[2] = {-1, -1};
static volatile sig_atomic_t g_stop = 0;

static void on_signal(int) {
    g_stop = 1;
    if (g_shutdown_pipe[1] != -1) {
        ssize_t r = ::write(g_shutdown_pipe[1], "x", 1);
        (void)r;
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0]
                  << " \"host=127.0.0.1 port=5432 dbname=kvstore user=$(whoami)\"\n";
        return 1;
    }
    const std::string conninfo = argv[1];

    // Writing to a socket whose peer has closed raises SIGPIPE, which kills the
    // process by default.  Ignore it and handle the EPIPE from write() instead.
    ::signal(SIGPIPE, SIG_IGN);

    if (::pipe(g_shutdown_pipe) != 0) { perror("pipe"); return 1; }
    struct sigaction sa {};
    sa.sa_handler = on_signal;
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);

    try {
        PgPool pool(conninfo, cfg::DB_POOL_SIZE);
        Storage storage(pool);
        storage.init_schema();

        ShardedClockCache cache(cfg::NUM_SHARDS, cfg::CACHE_CAPACITY);
        WalWriter wal("kvstore.wal");
        BoundedQueue queue(cfg::ACCEPT_QUEUE_CAP);
        Server server{cache, storage, wal, queue};

        unsigned hw = std::thread::hardware_concurrency();
        // Workers exceed core count on purpose: they block on DB I/O, so extra
        // threads keep the CPU busy.  But not by much -- context switching is
        // not free, and every worker competes for the same small pool.
        size_t worker_count = std::max<size_t>(4, (hw ? hw : 4) * 2);

        int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) { perror("socket"); return 1; }
        int opt = 1;
        ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(cfg::PORT);
        if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            perror("bind"); ::close(listen_fd); return 1;
        }
        if (::listen(listen_fd, 512) < 0) {
            perror("listen"); ::close(listen_fd); return 1;
        }

        std::vector<std::thread> workers;
        workers.reserve(worker_count);
        for (size_t i = 0; i < worker_count; ++i)
            workers.emplace_back([&server] { server.worker_loop(); });

        std::cout << "listening on :" << cfg::PORT
                  << "  workers=" << worker_count
                  << "  shards=" << cfg::NUM_SHARDS
                  << "  db_pool=" << cfg::DB_POOL_SIZE << "\n";

        int maxfd = std::max(listen_fd, g_shutdown_pipe[0]);
        while (!g_stop) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listen_fd, &rfds);
            FD_SET(g_shutdown_pipe[0], &rfds);

            int sel = ::select(maxfd + 1, &rfds, nullptr, nullptr, nullptr);
            if (sel < 0) {
                if (errno == EINTR) continue;
                perror("select");
                break;
            }
            if (FD_ISSET(g_shutdown_pipe[0], &rfds)) break;
            if (!FD_ISSET(listen_fd, &rfds)) continue;

            int cfd = ::accept(listen_fd, nullptr, nullptr);
            if (cfd < 0) {
                if (errno == EINTR || errno == ECONNABORTED) continue;
                perror("accept");
                continue;
            }
            if (!queue.push(cfd)) {
                // Overloaded: answer honestly and close rather than queueing
                // work we cannot get to.
                g_metrics.queue_rejects.fetch_add(1, std::memory_order_relaxed);
                send_response(cfd, 503, "server busy\n", false);
                ::close(cfd);
            }
        }

        std::cout << "\nshutting down (queued=" << queue.size() << ")\n";
        queue.stop();
        for (auto& t : workers) if (t.joinable()) t.join();
        ::close(listen_fd);
        ::close(g_shutdown_pipe[0]);
        ::close(g_shutdown_pipe[1]);
        std::cout << "clean exit\n";
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}