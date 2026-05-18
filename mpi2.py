# pyrefly: ignore [missing-import]
from mpi4py import MPI
import os
import time
from collections import Counter


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


def cargar_consulta(consulta_path, case_sensitive=False):
    """
    Lee consulta.txt y devuelve un conjunto de palabras objetivo.
    """
    if not os.path.isfile(consulta_path):
        raise FileNotFoundError(f"No se encontró el archivo de consulta: {consulta_path}")

    with open(consulta_path, "r", encoding="utf-8") as f:
        palabras = [line.strip() for line in f if line.strip()]

    if not case_sensitive:
        palabras = [w.lower() for w in palabras]

    return set(palabras)

def obtain_files(dataset_dir):
    """
    Returns a list with the path of every file_*.txt file within the dataset_dir directory.
    """
    file_paths = []

    for fname in os.listdir(dataset_dir):
        if not fname.startswith("file_") or not fname.endswith(".txt"):
            continue

        path = os.path.join(dataset_dir, fname)
        if not os.path.isfile(path):
            continue

        file_paths.append(path)

    return file_paths

def count_words_in_file(query_words, file_path, case_sensitive=False):
    local_counts= Counter()
    read_tokens = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            words = line.split()

            if not case_sensitive:
                words = [w.lower() for w in words]

            read_tokens += len(words)

            for w in words:
                if w in query_words:
                    local_counts[w] += 1

    return local_counts, read_tokens

def save_results_csv(out_path, counts):
    """
    Saves global counts in CSV defined path.
    """
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("palabra,conteo\n")
        for palabra in sorted(counts):
            f.write(f"{palabra},{counts[palabra]}\n")



def main ():

    consulta_name = "consulta.txt"
    case_sensitive = False
    top_n = 10
    output_file = "mpi2_results.csv"

    global_counts = Counter()
    total_tokens = 0
    total_files = 0
    worker_stats = {}

    script_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_dir = os.path.join(script_dir, "dataset")
    consulta_path = os.path.join(dataset_dir, consulta_name)

    t0 = time.perf_counter()

    # rank 0 reads consulta.txt
    if rank == 0:
        query_words = cargar_consulta(consulta_path, case_sensitive)
    else:
        query_words = None

    # rank 0 broadcasts the query words to all processes
    query_words = comm.bcast(query_words, root=0)


    if rank == 0:
        file_queue = obtain_files(dataset_dir)

        # give an initial file to every worker
        for i in range(1, size):
            comm.send(file_queue.pop(), dest=i)

        # main loop: provide to the first worker to finish
        status = MPI.Status()
        while file_queue:
            counts = comm.recv(source=MPI.ANY_SOURCE, status=status)
            global_counts.update(counts)
            src = status.Get_source()
            comm.send(file_queue.pop(), dest=src)

        # end of queue: 
        for w in range(1, size):
            counts = comm.recv(source=w)
            global_counts.update(counts)
            comm.send(None, dest=w) #STOP

        # gather per-worker stats
        for w in range(1, size):
            worker_stats[w] = comm.recv(source=w, tag=1)


    # workers
    else:
        local_tokens = 0
        local_files = 0
        local_time = time.perf_counter()

        while True:
            path = comm.recv(source=0)
            if path is None:  # rank 0 is telling to stop working. 
                break

            counts, tokens = count_words_in_file(query_words, path, case_sensitive)
            local_tokens += tokens
            local_files += 1
            comm.send(counts, dest=0)

        local_time = time.perf_counter() - local_time
        comm.send((local_tokens, local_files, local_time), dest=0, tag = 1)

    # 7. rank 0 builds the global result and prints the top 10.
    if rank == 0:
        global_elapsed = time.perf_counter() - t0
        
        out_path = os.path.join(dataset_dir, output_file)
        save_results_csv(out_path, global_counts)

        for w, (tokens, files, t) in worker_stats.items():
            print(f"  Worker {w}: {files} files - {t:.6f}s - {tokens} tokens")
            total_files += files
            total_tokens += tokens

        print(f"\nEXECUTION_TIME= {global_elapsed:.6f} segundos")
        print(f"Dataset procesado: {dataset_dir}")
        print(f"Archivo de consulta: {consulta_name}")
        print(f"Archivos procesados: {total_files}")
        print(f"Total de tokens leídos: {total_tokens}")
        print(f"Total de ocurrencias encontradas: {sum(global_counts.values())}")
        print(f"Resultados guardados en: {out_path}\n")

        print(f"Top {top_n} palabras de consulta en el corpus:")
        for palabra, cuenta in global_counts.most_common(top_n):
            print(f"  {palabra}: {cuenta}")



if __name__ == "__main__":
    main()