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

def count_words_in_chunk(query_words, file_paths, case_sensitive=False):
    local_counts= Counter()
    processed_files = len(file_paths)
    read_tokens = 0

    for path in file_paths:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                words = line.split()

                if not case_sensitive:
                    words = [w.lower() for w in words]

                read_tokens += len(words)

                for w in words:
                    if w in query_words:
                        local_counts[w] += 1

    return local_counts, processed_files, read_tokens

def merge_counters(a, b, datatype):
    for key in b:
        a[key] = a.get(key, 0) + b[key]
    return a

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
    output_file = "mpi1_results.csv"

    script_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_dir = os.path.join(script_dir, "dataset")
    consulta_path = os.path.join(dataset_dir, consulta_name)

    # 1. rank 0 reads consulta.txt
    if rank == 0:
        query_words = cargar_consulta(consulta_path, case_sensitive)
    else:
        query_words = None

    # 2. rank 0 broadcasts the query words to all processes using broadcast
    query_words = comm.bcast(query_words, root=0)

    if rank == 0:

        # 3. rank 0 obtains the list of file_*.txt files
        file_paths = obtain_files(dataset_dir)

    # 4. the files are distributed statically among the processes
        files_per_node = len(file_paths) // (size)
        remaining = len(file_paths) % (size) # there can be up to (size - 1) remainders

        start = 0

        for i in range(size):

            extra = 1 if i < remaining else 0 # take 1 extra file per process, until none remains.

            end = start + files_per_node + extra

            chunk = file_paths[start:end]

            if i == 0:
                assigned_files = chunk
            else:
                comm.send(chunk, dest=i)
            start = end    
    else:
        assigned_files = comm.recv(source=0)

    comm.barrier()
    t0 = time.perf_counter() # for global time

    # 5. each process counts locally the occurrences of the query words in its assigned files

    try:
        local_counts, processed_files, read_tokens = count_words_in_chunk(
            query_words,
            assigned_files,
            case_sensitive
        )
    except FileNotFoundError as e:
        print("Error:", e)
        return

    t1 = time.perf_counter()
    local_elapsed = t1 - t0

    print(f"Process {rank}: {len(assigned_files)} files - {local_elapsed:.6f}s - {read_tokens} tokens")


    # 6. partial results are gathered in rank 0

    # Create a custom op handler that can be used in a MPI.Reduce
    # commute = True takes advantage of commutativity and associativity
    # to alter the order of evaluation (otherwise, it'd be ordered by rank).
    merge_op = MPI.Op.Create(merge_counters, commute=True)

    #Use custom op in reduce to calculate global counts
    global_counts = comm.reduce(local_counts, op=merge_op, root=0)

    total_read_tokens = comm.reduce(read_tokens, op=MPI.SUM, root=0)
    total_processed_files = comm.reduce(processed_files, op=MPI.SUM, root=0)

    # 7. rank 0 builds the global result and prints the top 10.
    if rank == 0:
        global_elapsed = time.perf_counter() - t0
        out_path = os.path.join(dataset_dir, output_file)
        save_results_csv(out_path, global_counts)

        print(f"\nTiempo de ejecución: {global_elapsed:.6f} segundos")
        print(f"Dataset procesado: {dataset_dir}")
        print(f"Archivo de consulta: {consulta_name}")
        print(f"Archivos procesados: {total_processed_files}")
        print(f"Total de tokens leídos: {total_read_tokens}")
        print(f"Total de ocurrencias encontradas: {sum(global_counts.values())}")
        print(f"Resultados guardados en: {out_path}\n")

        print(f"Top {top_n} palabras de consulta en el corpus:")
        for palabra, cuenta in global_counts.most_common(top_n):
            print(f"  {palabra}: {cuenta}")



if __name__ == "__main__":
    main()
