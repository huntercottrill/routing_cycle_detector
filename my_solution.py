import os
import shutil
import sys
import tempfile
import zlib

NUM_BUCKETS = 256


def bucket_index(claim_id, status):
    return zlib.crc32(f"{claim_id}|{status}".encode("utf-8")) % NUM_BUCKETS


def partition_into_buckets(input_path, tmpdir):
    bucket_paths = [os.path.join(tmpdir, f"b{i}.txt") for i in range(NUM_BUCKETS)]
    outs = [
        open(p, "w", encoding="utf-8", newline="\n", buffering=1024 * 1024)
        for p in bucket_paths
    ]
    try:
        with open(input_path, "r", encoding="utf-8", buffering=1024 * 1024) as inf:
            for raw in inf:
                line = raw.strip()
                if not line:
                    continue
                parts = line.split("|")
                if len(parts) != 4:
                    continue
                _, _, claim_id, status = parts
                outs[bucket_index(claim_id, status)].write(line + "\n")
    finally:
        for f in outs:
            f.close()


def find_longest_cycle(adjacency_list):
    longest_cycle = 0

    for start_node in adjacency_list:
        path = []
        visited_in_path = set()

        def df_search(curr_node):
            nonlocal longest_cycle

            path.append(curr_node)
            visited_in_path.add(curr_node)

            for nxt in adjacency_list.get(curr_node, []):
                if nxt in visited_in_path:
                    cycle_start_index = path.index(nxt)
                    cycle_len = len(path) - cycle_start_index
                    if cycle_len > longest_cycle:
                        longest_cycle = cycle_len
                else:
                    df_search(nxt)

            path.pop()
            visited_in_path.remove(curr_node)

        df_search(start_node)

    return longest_cycle


def process_sorted_group(rows, best):
    """rows: (claim_id, status, src, dest) all same (claim_id, status)."""
    if not rows:
        return
    claim_id, status = rows[0][0], rows[0][1]
    system_to_id = {}
    nxt_id = 0

    def sid(s):
        nonlocal nxt_id
        if s not in system_to_id:
            system_to_id[s] = nxt_id
            nxt_id += 1
        return system_to_id[s]

    adj = {}
    for _, _, src, dest in rows:
        a, b = sid(src), sid(dest)
        adj.setdefault(a, []).append(b)

    cyc_len = find_longest_cycle(adj)
    if cyc_len > best[2]:
        best[0], best[1], best[2] = claim_id, status, cyc_len


def process_bucket_file(bucket_path, best):
    if not os.path.getsize(bucket_path):
        return
    with open(bucket_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    parsed = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        p = line.split("|")
        if len(p) != 4:
            continue
        src, dest, claim_id, status = p
        parsed.append((claim_id, status, src, dest))
    parsed.sort(key=lambda t: (t[0], t[1]))

    i, n = 0, len(parsed)
    while i < n:
        j = i + 1
        while j < n and parsed[j][0] == parsed[i][0] and parsed[j][1] == parsed[i][1]:
            j += 1
        process_sorted_group(parsed[i:j], best)
        i = j


def resolve_input_path(arg):
    if arg.startswith(("http://", "https://")):
        import urllib.request

        fd, path = tempfile.mkstemp(suffix=".txt", text=True)
        os.close(fd)
        urllib.request.urlretrieve(arg, path)
        return path, True
    return arg, False


def main():
    if len(sys.argv) < 2:
        print("Usage: python find_longest_claim_cycle.py <input_file_or_url>", file=sys.stderr)
        sys.exit(1)

    input_path, remove_input = resolve_input_path(sys.argv[1])
    tmpdir = tempfile.mkdtemp(prefix="route_cycles_")
    best = ["", "", 0]  # claim_id, status, length

    try:
        partition_into_buckets(input_path, tmpdir)
        for i in range(NUM_BUCKETS):
            process_bucket_file(os.path.join(tmpdir, f"b{i}.txt"), best)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        if remove_input:
            try:
                os.remove(input_path)
            except OSError:
                pass

    print(f"{best[0]},{best[1]},{best[2]}")


if __name__ == "__main__":
    main()
