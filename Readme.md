# Parallel Word Counting in a Text Corpus with MPI 
Team members:
- Juan Borja.
- Samuel Camargo.
- Fatima Castro.
- Juan Rojas.
  
---
## Problem Description
This lab addresses the problem of counting how many times each word from a query list (consulta.txt) appears across a corpus of text files (file_XXXX.txt), reporting the top 10 most frequent words. The solution is developed in three stages: a provided sequential implementation serves as the correctness and timing baseline, followed by a first MPI version that broadcasts query words to all processes and distributes corpus files statically among them, and finally a second MPI version that improves upon the first by reducing load imbalance. Both parallel implementations are experimentally evaluated with p ∈ {1, 2, 4, 8} processes and compared against the baseline using speedup, efficiency, and load balance metrics.

---
## Environment and Execution Instructions
All implementations run inside a Docker container provided for this lab, which includes Python and MPI pre-configured. No local installation of MPI or additional dependencies is required.
- ## Step 1 - Generate the dataset
  Run the generator script to create the dataset/ folder containing consulta.txt and the corpus files:
  ```
  #Linux/Mac
  docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 python /app/generator.py

  #Windows
  docker run --rm -v "%cd%:/app" augustosalazar/slim-mpi:2 python /app/generator.py
  ```
- ## Step 2 — Run the MPI implementations
  ```
  #MPI Version 1
  docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2  mpiexec --allow-run-as-root -n <p> --oversubscribe python /app/mpi1.py

  #MPI Version 2
  docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2  mpiexec --allow-run-as-root -n <p> --oversubscribe python /app/mpi2.py
  ```
  Replace p with the number of processes: 1, 2, 4, or 8.
---
## Experimental plan
- ## Sequential Baseline
  **What it does:** It reads the query words from consulta.txt, then scans every file_XXXX.txt in the dataset/ folder counting how many times each query word appears across the entire corpus. At the end, it prints the top 10 most frequent words and saves the full results to baseline_results.csv.
  
  **How it works:**
  
  **1.** Loads consulta.txt into a set of target words (lowercased for case-insensitive matching).
  
  **2.** Iterates over every file_*.txt file in the dataset directory one by one.
  
  **3.** For each file, reads it line by line, splits each line into tokens, and checks whether each token belongs to the query set — if so, increments its count using a Counter.
  
  **4.** Once all files are processed, it sorts the counts and prints the top 10 results, then writes the complete word-count table to baseline_results.csv.
  
  **5.** Execution time is measured with time.perf_counter() and reported as T_seq, the reference time used to compute speedup in the MPI experiments.
- ## MPI version 1
  **What it does:** It solves the same word-counting problem as the baseline but distributes the work across multiple processes using MPI. Rank 0 coordinates the work: it reads the query words, splits the corpus files among all processes, and collects the partial results to produce the global top 10.

  **How it works:**

  **1.** Rank 0 reads consulta.txt and broadcasts the query word set to all processes using comm.bcast(), so every process knows what to look for.
  
  **2.** Rank 0 lists all file_*.txt files and distributes them statically using integer division — each process gets total_files // p files, and the first total_files % p processes receive one extra file to absorb the remainder. Chunks are sent to each process via comm.send() / comm.recv().
  
  **3.** All processes synchronize at a comm.barrier(), then each one starts its local timer and counts word occurrences in its assigned files using a Counter, reading line by line and matching tokens against the query set.
  
  **4.** Each process prints its assigned file count, local processing time, and token count — which allows load imbalance to be observed directly.
  
  **5.** Partial Counter results are merged back at rank 0 using comm.reduce() with a custom commutative MPI operator that sums counters by key.
  
  **6.** Rank 0 computes the total elapsed time, prints the top 10 most frequent words, and saves the full results to mpi1_results.csv.
- ## Test procedure
  The dataset was generated once using the provided generator script. For each MPI version, the tests were conducted by varying the number of processes p ∈ {1, 2, 4, 8}, either by running the implementation directly or using the provided run_all.sh script:
  ```
  #Run a specific configuration
  docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 mpirun -n <p> python /app/mpi1.py

  #Or run all configurations at once
  docker run --rm -v "$(pwd)":/app augustosalazar/slim-mpi:2 bash /app/run_all.sh
  ```
---
## Experimental plan execution
- # Sequential baseline timing
