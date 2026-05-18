# Parallel Word Counting in a Text Corpus with MPI

Team members:
- Juan Borja.
- Samuel Camargo.
- Fatima Castro.
- Juan Rojas.

---

## 1. Problem Description

This lab addresses the problem of counting how many times each word from a query list (consulta.txt) appears across a corpus of text files (file_XXXX.txt), reporting the top 10 most frequent words. The solution is developed in three stages: a provided sequential implementation serves as the correctness and timing baseline, followed by a first MPI version that broadcasts query words to all processes and distributes corpus files statically among them, and finally a second MPI version that improves upon the first by reducing load imbalance. Both parallel implementations are experimentally evaluated with p ∈ {1, 2, 4, 8} processes and compared against the baseline using speedup, efficiency, and load balance metrics.

---

## 2. Environment and Execution Instructions

All implementations run inside a Docker container provided for this lab, which includes Python and MPI pre-configured. No local installation of MPI or additional dependencies is required.

### Step 2.1 — Generate the dataset

Run the generator script to create the dataset/ folder containing consulta.txt and the corpus files:

```bash
# Linux/Mac
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 python /app/generator.py

# Windows
docker run --rm -v "%cd%:/app" augustosalazar/slim-mpi:2 python /app/generator.py
```

### Step 2.2 — Run the MPI implementations

```bash
# MPI Version 1
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 mpiexec --allow-run-as-root -n <p> --oversubscribe python /app/mpi1.py

# MPI Version 2
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 mpiexec --allow-run-as-root -n <p> --oversubscribe python /app/mpi2.py
```

Replace `p` with the number of processes: 1, 2, 4, or 8.

---

## 3. Experimental Plan

### 3.a Sequential Baseline

**What it does:** It reads the query words from consulta.txt, then scans every file_XXXX.txt in the dataset/ folder counting how many times each query word appears across the entire corpus. At the end, it prints the top 10 most frequent words and saves the full results to baseline_results.csv.

**How it works:**

1. Loads consulta.txt into a set of target words (lowercased for case-insensitive matching).
2. Iterates over every file_*.txt file in the dataset directory one by one.
3. For each file, reads it line by line, splits each line into tokens, and checks whether each token belongs to the query set — if so, increments its count using a Counter.
4. Once all files are processed, it sorts the counts and prints the top 10 results, then writes the complete word-count table to baseline_results.csv.
5. Execution time is measured with time.perf_counter() and reported as T_seq, the reference time used to compute speedup in the MPI experiments.

---

### 3.b MPI Version 1

**What it does:** It solves the same word-counting problem as the baseline but distributes the work across multiple processes using MPI. Rank 0 coordinates the work: it reads the query words, splits the corpus files among all processes, and collects the partial results to produce the global top 10.

**How it works:**

1. Rank 0 reads consulta.txt and broadcasts the query word set to all processes using comm.bcast(), so every process knows what to look for.
2. Rank 0 lists all file_*.txt files and distributes them statically using integer division — each process gets total_files // p files, and the first total_files % p processes receive one extra file to absorb the remainder. Chunks are sent to each process via comm.send() / comm.recv().
3. Each process receives its assigned file list via comm.scatter(), immediately starts its local timer with time.perf_counter(), and counts word occurrences in its assigned files using a Counter, reading each file line by line, splitting into tokens, lowercasing them, and incrementing the count for any token found in the query set.
4. Each process prints its assigned file count, local processing time, and token count — which allows load imbalance to be observed directly.
5. Partial Counter results are merged back at rank 0 using comm.reduce() with a custom commutative MPI operator that sums counters by key.
6. Rank 0 computes the total elapsed time, prints the top 10 most frequent words, and saves the full results to mpi1_results.csv.

---

### 3.c Test Procedure

The dataset was generated once using the provided generator script. Before running any experiment, the number of available cores in the container was checked with:

```bash
docker run --rm augustosalazar/slim-mpi:2 nproc
```

> **Result: 8 cores available.** This means the container exposes 8 logical CPUs, which directly bounded our experimental configurations to p ∈ {1, 2, 4, 8}. Running more processes than available cores would force MPI to oversubscribe (multiple ranks sharing a single core), which introduces context-switching overhead and distorts timing measurements — this is why 8 was chosen as the upper limit.

For each MPI version, the tests were conducted by varying the number of processes p ∈ {1, 2, 4, 8}, either by running the implementation directly or using the provided run_all.sh script:

```bash
# Run a specific configuration
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 mpirun -n <p> python /app/mpi1.py

# Or run all configurations at once
docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 bash /app/run_all.sh
```

---

## 4. Experimental Plan Execution

- Every configuration was executed at least **3 times** to reduce measurement noise.
- For each run, **total wall-clock time** and **per-rank local processing time** were recorded.
- The **average** of the 3 runs was used for all metric computations.

Speedup and Efficiency are defined as:

$$S_p = \frac{T_{seq}}{T_p} \qquad\qquad E_p = \frac{S_p}{p}$$

where $T_{seq}$ is the sequential execution time and $T_p$ is the average MPI total time with $p$ processes.

**Process counts tested:** p ∈ { 1, 2, 4, 8 }

---

### 4.a Sequential Baseline Timing

| Metric | Value |
|---|---|
| **T_seq** | **7.9881 s** |
| Total tokens processed | 44,951,458 |

---

### 4.b MPI Version 1 Timing Results

#### p = 1

| Rank | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg (s) |
|:---:|---:|---:|---:|---:|
| 0 | 8.4887 | 8.4833 | 8.4936 | 8.4885 |
| **Total** | **8.4988** | **8.4939** | **8.5038** | **8.4988** |

#### p = 2

| Rank | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg (s) | Tokens |
|:---:|---:|---:|---:|---:|---:|
| 0 | 4.1980 | 4.1958 | 4.3082 | 4.2340 | 22,960,683 |
| 1 | 4.1177 | 4.1271 | 4.2173 | 4.1540 | 21,990,775 |
| **Total** | **4.2097** | **4.2072** | **4.3189** | **4.2453** | 44,951,458 |

#### p = 4

| Rank | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg (s) | Tokens |
|:---:|---:|---:|---:|---:|---:|
| 0 | 2.1872 | 2.2351 | 2.2388 | 2.2204 | 10,803,859 |
| 1 | 2.5124 | 2.6180 | 2.5770 | **2.5691**| 12,156,824 |
| 2 | 2.2435 | 2.3021 | 2.3584 | 2.3013 | 10,816,979 |
| 3 | 2.2917 | 2.3414 | 2.4269 | 2.3533 | 11,173,796 |
| **Total** | **2.5238** | **2.6295** | **2.5880** | **2.5804** | 44,951,458 |

#### p = 8

| Rank | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg (s) | Tokens |
|:---:|---:|---:|---:|---:|---:|
| 0 | 2.3576 | 2.4624 | 2.5012 | 2.4404 | 6,002,531 |
| 1 | 1.9789 | 2.0042 | 2.1707 | **2.0513** | 4,801,328 |
| 2 | 2.5119 | 2.7368 | 2.7786 | **2.6758** | 6,180,576 |
| 3 | 2.4680 | 2.4198 | 2.6028 | 2.4969 | 5,976,248 |
| 4 | 2.1888 | 2.1992 | 2.3329 | 2.2403 | 5,281,651 |
| 5 | 2.3948 | 2.2019 | 2.2405 | 2.2790 | 5,535,328 |
| 6 | 2.2154 | 2.2660 | 2.3839 | 2.2884 | 5,409,609 |
| 7 | 2.4111 | 2.4258 | 2.4639 | 2.4336 | 5,764,187 |
| **Total** | **2.5350** | **2.7915** | **2.8013** | **2.7093** | 44,951,458 |

#### MPI Version 1 — Performance Summary

| p | T_p avg (s) | Speedup S_p | Efficiency E_p |
|:---:|---:|---:|---:|
| 1 | 8.4988 | 0.9399 | 0.9399 |
| 2 | 4.2453 | 1.8817 | 0.9408 |
| 4 | 2.5804 | 3.0957 | 0.7739 |
| 8 | 2.7093 | 2.9484 | 0.3686 |

T_seq = **7.9881 s** — used as the reference for all speedup calculations.

---

### 4.c Load Imbalance Evidence

The per-rank token and timing data from Version 1 reveal clear load imbalance:

**At p = 4:**
- Rank 1 processed **12,156,824 tokens** vs Rank 0's **10,803,859** — a **~12.5% difference**.
- Rank 1's average time (2.5691 s) was **~16% slower** than Rank 0 (2.2204 s).
- Total wall-clock time is bottlenecked by the slowest rank, wasting idle cycles in faster ranks.

**At p = 8:**
- Token counts ranged from **4,801,328** (Rank 1) to **6,180,576** (Rank 2) — a **~28.7% spread**.
- Rank 2 (2.6758 s) took **0.62 s longer** than Rank 1 (2.0513 s).
- This imbalance explains why going from p = 4 to p = 8 produced **no speedup improvement** (T_p increased from 2.5804 s to 2.7093 s).

Static file-count distribution is insufficient when corpus files vary significantly in size.

---

### 4.d MPI Version 2 — Load-Balanced Distribution

`mpi2.py` replaces the static file-count split with a **token-aware greedy assignment**:

1. **Rank 0** reads and computes token counts for all corpus files before distributing work.
2. Files are assigned to worker ranks greedily so that each rank accumulates a similar total token load.
3. All other steps (broadcast, local counting, gather, merge) remain identical to Version 1.

Note: p = 1 is not valid in this implementation, as Rank 0 acts as coordinator only — all work is distributed exclusively among worker ranks (Ranks 1 through p-1).

#### p = 2

| Rank | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg (s) | Files (all runs) | Tokens (all runs) |
|:---:|---:|---:|---:|---:|---:|---:|
| 0 (coord.) | — | — | — | — | — | — |
| 1 | 9.6205 | 9.5208 | 9.2813 | 9.4742 | 3,000 | 44,951,458 |
| **Total** | **9.6207** | **9.5222** | **9.2815** | **9.4748** | — | — |

Only one worker handles all files when p = 2, negating parallelism.

#### p = 4

| Rank | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg (s) | Files R1 | Files R2 | Files R3 |
|:---:|---:|---:|---:|---:|---:|---:|---:|
| 0 (coord.) | — | — | — | — | — | — | — |
| 1 | 3.4109 | 3.5539 | 3.6548 | 3.5399 | 1,070 | 1,028 | 953 |
| 2 | 3.4206 | 3.5632 | 3.6549 | 3.5462 | 942 | 981 | 1,060 |
| 3 | 3.4207 | 3.5632 | 3.6550 | 3.5463 | 988 | 991 | 987 |
| **Total** | **3.4210** | **3.5635** | **3.6553** | **3.5466** | **3,000** | **3,000** | **3,000** |

#### p = 8

| Rank | Run 1 (s) | Run 2 (s) | Run 3 (s) | Avg (s) | Files R1 | Files R2 | Files R3 |
|:---:|---:|---:|---:|---:|---:|---:|---:|
| 0 (coord.) | — | — | — | — | — | — | — |
| 1 | 2.8085 | 2.8352 | 2.8712 | 2.8383 | 425 | 423 | 432 |
| 2 | 2.8086 | 2.8344 | 2.8713 | 2.8381 | 459 | 362 | 402 |
| 3 | 2.8177 | 2.8357 | 2.8714 | 2.8416 | 385 | 431 | 365 |
| 4 | 2.8178 | 2.8347 | 2.8699 | 2.8408 | 373 | 422 | 390 |
| 5 | 2.8179 | 2.8430 | 2.8700 | 2.8436 | 438 | 462 | 461 |
| 6 | 2.8180 | 2.8431 | 2.8741 | 2.8450 | 450 | 474 | 466 |
| 7 | 2.8181 | 2.8431 | 2.8703 | 2.8438 | 470 | 426 | 484 |
| **Total** | **2.8185** | **2.8456** | **2.8768** | **2.8470** | **3,000** | **3,000** | **3,000** |

At p = 8, all worker ranks completed within **< 0.007 s of each other** — a dramatic improvement over Version 1's 0.62 s per-rank spread.

#### MPI Version 2 — Performance Summary

| p | T_p avg (s) | Speedup S_p | Efficiency E_p |
|:---:|---:|---:|---:|
| 2 | 9.4748 | 0.8431 | 0.4215 |
| 4 | 3.5466 | 2.2523 | 0.5631 |
| 8 | 2.8470 | 2.8058 | 0.3507 |

#### Full Comparison — MPI Version 1 vs Version 2

| p | V1 T_p (s) | V1 S_p | V1 E_p | V2 T_p (s) | V2 S_p | V2 E_p |
|:---:|---:|---:|---:|---:|---:|---:|
| 2 | 4.2453 | 1.8817 | 0.9408 | 9.4748 | 0.8431 | 0.4215 |
| 4 | 2.5804 | 3.0957 | 0.7739 | 3.5466 | 2.2523 | 0.5631 |
| 8 | 2.7093 | 2.9484 | 0.3686 | 2.8470 | 2.8058 | 0.3507 |

---

## 5. Analysis

**Did the first MPI implementation improve execution time compared to the sequential baseline?**

Yes, partially. Version 1 showed clear improvements at p = 2 (4.25 s vs 7.99 s) and reached its best at p = 4 (2.58 s, 3.1× speedup). However, at p = 8, performance degraded slightly compared to p = 4 (2.71 s), indicating that load imbalance cancelled the potential gains from adding more processes.

**Was the observed speedup linear?**

No. The closest result to linear speedup was p = 2 (S_p = 1.88, ideal = 2.0, efficiency = 0.94). By p = 4, efficiency dropped to 0.77, and at p = 8, it fell to 0.37. MPI coordination overhead, broadcast costs, and load imbalance all eroded ideal linear scaling.

**Is there evidence of load imbalance? How was it observed?**

Yes, clearly. In Version 1 at p = 4, Rank 1 held 12,156,824 tokens while Rank 0 held only 10,803,859 (~12.5% more work), causing Rank 1 to be ~16% slower. At p = 8, the token spread reached ~28.7% between the fastest and slowest ranks, with a 0.62 s timing gap. The total wall-clock time is always bottlenecked by the slowest rank, making this imbalance directly responsible for the performance plateau between p = 4 and p = 8.

**Did the second implementation reduce load imbalance?**

Yes. At p = 8, all worker ranks in Version 2 completed within less than 0.007 s of each other, compared to a 0.62 s spread in Version 1. The token-aware greedy assignment successfully equalised workload across all ranks.

**Did the improved distribution strategy produce a real performance improvement?**

Not in absolute wall-clock time. Version 2 was consistently slower than Version 1 across all tested process counts. While Version 2 achieved near-perfect load balance, the overhead of computing and coordinating the token-aware assignment, plus the fact that Rank 0 acts as a pure coordinator (adding an idle resource), outweighed the gains from better balance. Version 1's best result (2.58 s at p = 4) outperformed Version 2's best (2.85 s at p = 8). Version 2's design would show its advantage with a corpus with much higher file-size variance, where imbalance would dominate.

**What limitations affected your experiment?**

- The container was limited to **8 logical cores** (`nproc` returned 8), preventing evaluation beyond p = 8.
- All processes ran on the **same physical machine** (shared memory bus, shared cache), hiding true distributed-memory MPI latency and overhead.
- **Docker startup noise** and OS scheduling introduced non-deterministic variation across runs.
- File-system caching (warm cache after run 1) likely deflated I/O times in subsequent runs.
- In Version 2 with p = 2, only a single worker rank handles all 3,000 files, effectively eliminating parallelism and explaining its 9.47 s time.

---

## 6. Conclusions

**Did parallel implementations improve execution time over the sequential baseline?**

Version 1 delivered meaningful speedup at p = 2 and p = 4, reducing execution time from 7.99 s to 2.58 s (3.1× speedup). Version 2 also improved over the sequential baseline at p = 4 and p = 8, but never surpassed Version 1's results in absolute wall-clock time.

**Most important problem observed in the first parallel version:**

Load imbalance caused by static file-count distribution. Since corpus files vary in size, some ranks consistently received heavier workloads. At p = 8, this imbalance was severe enough that adding more processes over p = 4 actually increased total execution time instead of reducing it.

**Did the second version help and how?**

Version 2 solved the load balance problem: at p = 8, per-rank execution times were nearly identical (< 0.007 s variance), confirming the token-aware greedy assignment works correctly. However, the additional coordination cost made it slower than Version 1 in absolute terms on this dataset.

**Evidence-based judgment:**

For this corpus and container environment, **MPI Version 1 at p = 4 delivered the best overall performance** (2.58 s, 3.1× speedup, 0.77 efficiency). Version 2 is architecturally superior for datasets with high file-size variance, where load imbalance would dominate over assignment overhead. Both versions confirm that MPI parallelism is effective for large-scale text-processing tasks, and that workload distribution strategy is as important as the degree of parallelism itself.
