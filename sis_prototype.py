"""
Student Information Processing Architecture - Month 2 prototype (Group 24)

Simulates the four processing units from the architecture diagram:
    Core 1 - grade computation
    Core 2 - attendance aggregation
    Core 3 - fee processing
    Core 4 - report generation
and compares sequential execution with task-level and data-level parallelism.
Run:  python sis_prototype.py      (writes results.json)
"""
import json, os, platform, random, sqlite3, statistics, sys, threading, time, hashlib
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from queue import Queue

SEED = 24
N_STUDENTS = 100_000     # CPU-bound experiment
N_LATENCY = 1_000        # I/O-latency experiment
DB_LATENCY = 0.002       # simulated 2 ms storage round-trip per record
DAYS = 90
SUBJECTS = 6
REPEATS = 3

# ---------------------------------------------------------------- dataset
def make_dataset(n, seed=SEED):
    rnd = random.Random(seed)
    return {
        "marks": [[(rnd.randint(8, 30), rnd.randint(15, 70)) for _ in range(SUBJECTS)] for _ in range(n)],
        "attendance": [rnd.getrandbits(DAYS) | rnd.getrandbits(DAYS) for _ in range(n)],  # 1 = present
        "fee": [(rnd.choice((95000, 105000, 120000)), rnd.randint(0, 120000)) for _ in range(n)],
    }

DATA = None  # global so forked workers inherit it without pickling

# ---------------------------------------------------------------- the four "cores"
def grade_one(i):
    total, gp_sum = 0, 0
    for internal, external in DATA["marks"][i]:
        m = internal + external
        total += m
        gp_sum += 10 if m >= 90 else 9 if m >= 80 else 8 if m >= 70 else 7 if m >= 60 else 6 if m >= 50 else 5 if m >= 40 else 0
    return (total, round(gp_sum / SUBJECTS, 2))

def attendance_one(i):
    present = bin(DATA["attendance"][i]).count("1")
    pct = round(100 * present / DAYS, 1)
    return (present, pct, pct < 75.0)

def fee_one(i):
    due, paid = DATA["fee"][i]
    return (max(due - paid, 0), paid >= due)

def report_one(i):
    g, a, f = grade_one(i), attendance_one(i), fee_one(i)
    return f"{i:06d}|{g[0]}|{g[1]}|{a[1]}|{'SHORT' if a[2] else 'OK'}|{f[0]}"

TASKS = {"grade": grade_one, "attendance": attendance_one, "fee": fee_one, "report": report_one}

def run_task(name, lo, hi):
    fn = TASKS[name]
    return [fn(i) for i in range(lo, hi)]

def digest(results):
    h = hashlib.sha256()
    for name in sorted(results):
        h.update(name.encode()); h.update(repr(results[name]).encode())
    return h.hexdigest()[:16]

# ---------------------------------------------------------------- execution modes
def sequential(n):
    return {name: run_task(name, 0, n) for name in TASKS}

def _chunk_job(args):
    name, lo, hi = args
    return name, lo, run_task(name, lo, hi)

def _task_job(args):
    name, n = args
    return name, run_task(name, 0, n)

def task_parallel(n, workers):
    """One worker per processing unit (grade / attendance / fee / report)."""
    ctx = mp.get_context("fork")
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as ex:
        return dict(ex.map(_task_job, [(t, n) for t in TASKS]))

def data_parallel(n, workers, chunk=2500):
    """Records split into small chunks; idle workers pull the next chunk (dynamic load balancing)."""
    ctx = mp.get_context("fork")
    jobs = [(t, lo, min(lo + chunk, n)) for t in TASKS for lo in range(0, n, chunk)]
    out = {t: {} for t in TASKS}
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as ex:
        for name, lo, res in ex.map(_chunk_job, jobs):
            out[name][lo] = res
    return {t: [r for lo in sorted(out[t]) for r in out[t][lo]] for t in TASKS}

def timed(fn, *a):
    best, res = None, None
    for _ in range(REPEATS):
        t0 = time.perf_counter(); res = fn(*a); dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return best, res

# ---------------------------------------------------------------- latency-bound experiment (threads)
def fetch_and_grade(i):
    time.sleep(DB_LATENCY)           # simulated round-trip to the central database
    return grade_one(i)

def latency_sequential(n):
    return [fetch_and_grade(i) for i in range(n)]

def latency_threads(n, workers):
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(fetch_and_grade, range(n)))

# ---------------------------------------------------------------- concurrency control: shared cache + single DB writer
class SharedCache:
    """Shared-memory/cache block from the architecture diagram, protected by a lock."""
    def __init__(self): self.d, self.hits, self.misses, self.lock = {}, 0, 0, threading.Lock()
    def get_or_compute(self, key, fn):
        with self.lock:
            if key in self.d:
                self.hits += 1; return self.d[key]
        val = fn(key)
        with self.lock:
            self.misses += 1; self.d.setdefault(key, val)
        return self.d[key]

def consistency_test(n=20000, workers=8):
    cache, q, written = SharedCache(), Queue(), []
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.execute("CREATE TABLE grades(id INTEGER PRIMARY KEY, total INT, sgpa REAL)")
    def writer():                    # single writer -> no write-write conflicts
        while True:
            item = q.get()
            if item is None: break
            db.execute("INSERT INTO grades VALUES(?,?,?)", item); written.append(item[0])
        db.commit()
    wt = threading.Thread(target=writer); wt.start()
    def work(i):
        g = cache.get_or_compute(i % 5000, grade_one)   # repeated ids exercise the cache
        q.put((i, g[0], g[1]))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(work, range(n)))
    q.put(None); wt.join()
    rows = db.execute("SELECT COUNT(*), COUNT(DISTINCT id) FROM grades").fetchone()
    ok = all(db.execute("SELECT total, sgpa FROM grades WHERE id=?", (i,)).fetchone() == grade_one(i % 5000)
             for i in (0, 1, 4999, 5000, 12345, n - 1))
    return {"records": n, "rows_written": rows[0], "distinct_ids": rows[1],
            "cache_hits": cache.hits, "cache_misses": cache.misses, "spot_checks_match": ok}

# ---------------------------------------------------------------- main
def main():
    global DATA
    env = {"python": platform.python_version(), "os": platform.platform(),
           "cpu_cores_available": len(os.sched_getaffinity(0)), "processor": platform.processor() or platform.machine()}
    print("environment:", env)
    DATA = make_dataset(N_STUDENTS)

    t_seq, ref = timed(sequential, N_STUDENTS)
    ref_digest = digest(ref)
    cpu = {"records": N_STUDENTS, "sequential_s": t_seq, "task_parallel": [], "data_parallel": []}
    for w in (2, 4):
        t, r = timed(task_parallel, N_STUDENTS, w)
        cpu["task_parallel"].append({"workers": w, "time_s": t, "speedup": t_seq / t, "identical": digest(r) == ref_digest})
    for w in (2, 4, 8):
        t, r = timed(data_parallel, N_STUDENTS, w)
        cpu["data_parallel"].append({"workers": w, "time_s": t, "speedup": t_seq / t, "identical": digest(r) == ref_digest})
    print("cpu:", json.dumps(cpu, indent=1))

    t0 = time.perf_counter(); base = latency_sequential(N_LATENCY); lat_seq = time.perf_counter() - t0
    lat = {"records": N_LATENCY, "db_latency_ms": DB_LATENCY * 1000, "sequential_s": lat_seq, "threads": []}
    for w in (2, 4, 8, 16):
        t0 = time.perf_counter(); r = latency_threads(N_LATENCY, w); dt = time.perf_counter() - t0
        lat["threads"].append({"workers": w, "time_s": dt, "speedup": lat_seq / dt, "identical": r == base})
    print("latency:", json.dumps(lat, indent=1))

    cons = consistency_test()
    print("consistency:", cons)
    json.dump({"env": env, "cpu": cpu, "latency": lat, "consistency": cons}, open("results.json", "w"), indent=2)

if __name__ == "__main__":
    main()
