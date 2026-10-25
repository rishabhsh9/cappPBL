# Student Information Processing Architecture - Prototype (Group 24)
Simulates the four processing units (grade, attendance, fee, report) on 100,000 synthetic student records and compares sequential, task-level parallel and data-level parallel runs.
Also tests a thread pool with simulated 2 ms database delay, and a lock-protected shared cache with a single SQLite writer.
Run: `python sis_prototype.py` (Python 3.8+ on Linux/macOS, no extra packages). Results are saved to results.json.
Each parallel run is checked against the sequential output with a SHA-256 digest. Timings are the best of 3 runs and depend on your CPU core count.
