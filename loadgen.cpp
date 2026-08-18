// =============================================================================
//  loadgen -- closed-loop load generator for the multi-tier KV server
//
//  Closed loop means each client thread sends one request, waits for the reply,
//  then sends the next.  So N threads = N outstanding requests, always.  That
//  is what makes Little's Law (N = throughput x latency) hold, and checking
//  that identity is the cheapest sanity check on a measurement run: if your
//  numbers do not satisfy it, the measurement is wrong, not the server.
//
//  Three things this measures that a naive generator misses:
//
//   * PERCENTILES, not just the mean.  An average hides the tail completely.
//     A server with a 5ms mean and a 900ms p99 is broken, and the mean will
//     never tell you.
//
//   * WARMUP is discarded.  The first seconds include connection setup, cold
//     caches, page faults, and (on a VM) the scheduler still deciding which
//     core you deserve.  Folding that into the steady-state number is the most
//     common way to produce a misleading benchmark.
//
//   * KEYSPACE SIZE is a parameter.  Cache behaviour is entirely determined by
//     working-set size versus cache capacity.  Sweeping --keyspace across the
//     server's capacity is how you produce a hit-ratio curve instead of one
//     arbitrary number.
//
//  Build:
//     g++ -std=c++17 -O2 -pthread loadgen.cpp -lcurl -o loadgen
//
//  Examples:
//     # read-heavy, working set fits in cache -> should show a high hit ratio
//     ./loadgen --clients 8 --duration 60 --workload mixed --keyspace 1000
//
//     # working set far exceeds cache -> hit ratio collapses, DB reads dominate
//     ./loadgen --clients 8 --duration 60 --workload mixed --keyspace 100000
//
//     # pure write path -- this is the one that exercises the WAL
//     ./loadgen --clients 16 --duration 60 --workload write
// =============================================================================

#include <curl/curl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

// ----------------------------------------------------------------------------
//  Configuration
// ----------------------------------------------------------------------------
struct Config {
    int         clients   = 8;
    int         duration  = 60;      // seconds of measured traffic
    int         warmup    = 5;       // seconds discarded before measuring
    std::string workload  = "mixed"; // read | write | mixed | delete
    int         read_pct  = 90;      // for "mixed"
    long        keyspace  = 1000;    // distinct keys touched
    int         valuesize = 64;      // bytes per value
    std::string base      = "http://127.0.0.1:8080";
    std::string csv       = "results.csv";
};

// ----------------------------------------------------------------------------
//  Per-thread results.  Kept thread-local and merged at the end so the hot loop
//  never touches a shared cache line -- a shared counter updated per request
//  would itself become a bottleneck and distort what we are measuring.
// ----------------------------------------------------------------------------
struct ThreadResult {
    std::vector<double> latencies_ms;  // steady-state samples only
    long long reads = 0, writes = 0, deletes = 0;
    long long http_2xx = 0, http_4xx = 0, http_5xx = 0, errors = 0;
};

static size_t discard_body(void*, size_t size, size_t nmemb, void*) {
    return size * nmemb;
}

// ----------------------------------------------------------------------------
//  One client thread.
//
//  The CURL handle is created once and reused for every request.  That is what
//  gives us HTTP keep-alive: libcurl holds the TCP connection open between
//  requests to the same host.  Creating a fresh handle per request would add a
//  TCP (and possibly TLS) handshake to every sample, and you would end up
//  benchmarking connection setup instead of the server.
// ----------------------------------------------------------------------------
static void client_thread(const Config& cfg, int thread_id, ThreadResult& out,
                          std::atomic<bool>& measuring, std::atomic<bool>& stop) {
    CURL* curl = curl_easy_init();
    if (!curl) return;

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discard_body);
    curl_easy_setopt(curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);  // reuse connections

    std::mt19937_64 rng(static_cast<uint64_t>(thread_id) * 0x9E3779B97F4A7C15ULL + 1);
    std::uniform_int_distribution<long> key_dist(0, std::max(0L, cfg.keyspace - 1));
    std::uniform_int_distribution<int> pct_dist(0, 99);

    const std::string value(static_cast<size_t>(cfg.valuesize), 'v');

    // Reserve generously so the vector does not reallocate mid-run; a realloc
    // inside the timed loop would show up as a latency spike that the server
    // never caused.
    out.latencies_ms.reserve(static_cast<size_t>(cfg.duration) * 2000);

    while (!stop.load(std::memory_order_relaxed)) {
        long key = key_dist(rng);
        std::string url = cfg.base + "/kv/k" + std::to_string(key);

        enum { OP_READ, OP_WRITE, OP_DELETE } op;
        if (cfg.workload == "read")        op = OP_READ;
        else if (cfg.workload == "write")  op = OP_WRITE;
        else if (cfg.workload == "delete") op = OP_DELETE;
        else op = (pct_dist(rng) < cfg.read_pct) ? OP_READ : OP_WRITE;

        // Reset the method-specific options every iteration.  Leaving a stale
        // CUSTOMREQUEST or POSTFIELDS on a reused handle silently sends the
        // wrong method -- a genuinely nasty bug to track down.
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, nullptr);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, nullptr);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, -1L);
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);

        switch (op) {
            case OP_READ:
                break;  // plain GET
            case OP_WRITE:
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, value.c_str());
                curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE,
                                 static_cast<long>(value.size()));
                break;
            case OP_DELETE:
                curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
                break;
        }

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

        auto t0 = Clock::now();
        CURLcode rc = curl_easy_perform(curl);
        auto t1 = Clock::now();

        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        // Samples taken during warmup are thrown away entirely -- they are not
        // steady state and averaging them in is how benchmarks lie.
        bool record = measuring.load(std::memory_order_relaxed);

        if (rc != CURLE_OK) {
            if (record) out.errors++;
            continue;
        }

        long code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);

        if (record) {
            out.latencies_ms.push_back(ms);
            if      (code >= 500) out.http_5xx++;
            else if (code >= 400) out.http_4xx++;
            else                  out.http_2xx++;

            if      (op == OP_READ)  out.reads++;
            else if (op == OP_WRITE) out.writes++;
            else                     out.deletes++;
        }
    }

    curl_easy_cleanup(curl);
}

// ----------------------------------------------------------------------------
//  Percentile from a sorted vector (nearest-rank).
// ----------------------------------------------------------------------------
static double percentile(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0.0;
    size_t idx = static_cast<size_t>(std::ceil(p / 100.0 * sorted.size()));
    if (idx == 0) idx = 1;
    if (idx > sorted.size()) idx = sorted.size();
    return sorted[idx - 1];
}

// ----------------------------------------------------------------------------
//  Fetch /metrics so the report shows what the SERVER saw, next to what the
//  CLIENT saw.  Disagreement between the two is informative: e.g. if the client
//  counts more requests than the server, work is being dropped somewhere.
// ----------------------------------------------------------------------------
static std::string fetch_metrics(const std::string& base) {
    CURL* curl = curl_easy_init();
    if (!curl) return "";
    std::string body;
    auto cb = +[](void* p, size_t sz, size_t nm, void* ud) -> size_t {
        static_cast<std::string*>(ud)->append(static_cast<char*>(p), sz * nm);
        return sz * nm;
    };
    std::string url = base + "/metrics";
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
    if (curl_easy_perform(curl) != CURLE_OK) body.clear();
    curl_easy_cleanup(curl);
    return body;
}

static void usage(const char* prog) {
    std::cerr <<
        "usage: " << prog << " [options]\n"
        "  --clients N      concurrent clients          (default 8)\n"
        "  --duration S     measured seconds            (default 60)\n"
        "  --warmup S       discarded seconds up front  (default 5)\n"
        "  --workload W     read | write | mixed | delete (default mixed)\n"
        "  --read-pct P     read share of 'mixed'       (default 90)\n"
        "  --keyspace N     distinct keys               (default 1000)\n"
        "  --valuesize B    value bytes                 (default 64)\n"
        "  --base URL       server base URL             (default http://127.0.0.1:8080)\n"
        "  --csv FILE       append summary row          (default results.csv)\n";
}

int main(int argc, char** argv) {
    Config cfg;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) { usage(argv[0]); std::exit(1); }
            return argv[++i];
        };
        if      (a == "--clients")   cfg.clients   = std::stoi(next());
        else if (a == "--duration")  cfg.duration  = std::stoi(next());
        else if (a == "--warmup")    cfg.warmup    = std::stoi(next());
        else if (a == "--workload")  cfg.workload  = next();
        else if (a == "--read-pct")  cfg.read_pct  = std::stoi(next());
        else if (a == "--keyspace")  cfg.keyspace  = std::stol(next());
        else if (a == "--valuesize") cfg.valuesize = std::stoi(next());
        else if (a == "--base")      cfg.base      = next();
        else if (a == "--csv")       cfg.csv       = next();
        else { usage(argv[0]); return 1; }
    }

    if (cfg.workload != "read" && cfg.workload != "write" &&
        cfg.workload != "mixed" && cfg.workload != "delete") {
        usage(argv[0]);
        return 1;
    }

    curl_global_init(CURL_GLOBAL_DEFAULT);

    // A read-only workload against an empty store measures nothing but 404s,
    // so seed the keyspace first.
    if (cfg.workload == "read" || cfg.workload == "mixed") {
        std::cout << "seeding " << cfg.keyspace << " keys...\n";
        CURL* c = curl_easy_init();
        std::string value(static_cast<size_t>(cfg.valuesize), 'v');
        curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, discard_body);
        curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(c, CURLOPT_POSTFIELDS, value.c_str());
        curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(value.size()));
        for (long k = 0; k < cfg.keyspace; ++k) {
            std::string url = cfg.base + "/kv/k" + std::to_string(k);
            curl_easy_setopt(c, CURLOPT_URL, url.c_str());
            curl_easy_perform(c);
        }
        curl_easy_cleanup(c);
    }

    std::cout << "clients=" << cfg.clients
              << " workload=" << cfg.workload
              << " keyspace=" << cfg.keyspace
              << " warmup=" << cfg.warmup << "s"
              << " duration=" << cfg.duration << "s\n";

    std::vector<ThreadResult> results(static_cast<size_t>(cfg.clients));
    std::atomic<bool> measuring{false}, stop{false};
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(cfg.clients));

    for (int i = 0; i < cfg.clients; ++i) {
        threads.emplace_back(client_thread, std::cref(cfg), i,
                             std::ref(results[static_cast<size_t>(i)]),
                             std::ref(measuring), std::ref(stop));
    }

    std::this_thread::sleep_for(std::chrono::seconds(cfg.warmup));
    std::cout << "warmup done, measuring...\n";

    auto t_start = Clock::now();
    measuring.store(true);
    std::this_thread::sleep_for(std::chrono::seconds(cfg.duration));
    measuring.store(false);
    auto t_end = Clock::now();

    stop.store(true);
    for (auto& t : threads) if (t.joinable()) t.join();

    // Merge per-thread samples.
    std::vector<double> all;
    long long reads = 0, writes = 0, deletes = 0;
    long long c2xx = 0, c4xx = 0, c5xx = 0, errs = 0;
    for (const auto& r : results) {
        all.insert(all.end(), r.latencies_ms.begin(), r.latencies_ms.end());
        reads += r.reads; writes += r.writes; deletes += r.deletes;
        c2xx += r.http_2xx; c4xx += r.http_4xx; c5xx += r.http_5xx;
        errs += r.errors;
    }
    std::sort(all.begin(), all.end());

    double elapsed = std::chrono::duration<double>(t_end - t_start).count();
    double total = static_cast<double>(all.size());
    double throughput = elapsed > 0 ? total / elapsed : 0.0;
    double mean = 0.0;
    for (double v : all) mean += v;
    if (!all.empty()) mean /= total;

    double p50 = percentile(all, 50);
    double p95 = percentile(all, 95);
    double p99 = percentile(all, 99);
    double pmax = all.empty() ? 0.0 : all.back();

    std::cout << std::fixed << std::setprecision(3)
              << "\n=== client-side ===\n"
              << "elapsed(s)        " << elapsed    << "\n"
              << "requests          " << total      << "\n"
              << "throughput(req/s) " << throughput << "\n"
              << "mean(ms)          " << mean       << "\n"
              << "p50(ms)           " << p50        << "\n"
              << "p95(ms)           " << p95        << "\n"
              << "p99(ms)           " << p99        << "\n"
              << "max(ms)           " << pmax       << "\n"
              << "reads/writes/dels " << reads << "/" << writes << "/" << deletes << "\n"
              << "2xx/4xx/5xx/err   " << c2xx << "/" << c4xx << "/" << c5xx << "/" << errs << "\n";

    // Little's Law: with a closed loop of N clients, N should equal
    // throughput x mean_latency.  A large deviation means the harness is the
    // problem (client CPU saturated, DNS lookups, connection churn) and the
    // server numbers should not be trusted until it is fixed.
    double little_n = throughput * (mean / 1000.0);
    std::cout << "littles_law_N     " << little_n
              << "  (expected " << cfg.clients << ")\n";
    if (cfg.clients > 0 && std::fabs(little_n - cfg.clients) / cfg.clients > 0.10) {
        std::cout << "  [warn] deviates >10% from client count -- suspect the harness\n";
    }

    std::string m = fetch_metrics(cfg.base);
    if (!m.empty()) std::cout << "\n=== server-side (/metrics) ===\n" << m;

    // Append one row per run so a sweep over --clients plots directly.
    bool need_header = true;
    { std::ifstream probe(cfg.csv); need_header = !probe.good() || probe.peek() == std::ifstream::traits_type::eof(); }
    std::ofstream f(cfg.csv, std::ios::app);
    if (f) {
        if (need_header) {
            f << "clients,workload,keyspace,elapsed_s,requests,throughput_rps,"
                 "mean_ms,p50_ms,p95_ms,p99_ms,max_ms,reads,writes,deletes,"
                 "http_2xx,http_4xx,http_5xx,errors\n";
        }
        f << cfg.clients << "," << cfg.workload << "," << cfg.keyspace << ","
          << elapsed << "," << total << "," << throughput << ","
          << mean << "," << p50 << "," << p95 << "," << p99 << "," << pmax << ","
          << reads << "," << writes << "," << deletes << ","
          << c2xx << "," << c4xx << "," << c5xx << "," << errs << "\n";
        std::cout << "\nappended to " << cfg.csv << "\n";
    }

    curl_global_cleanup();
    return 0;
}