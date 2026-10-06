"""Behavioral checks for the streaming engine on public and private inputs."""

from functools import lru_cache
from heapq import heappop, heappush
from pathlib import Path
import ctypes
import os
import pwd
import shutil
import signal
import subprocess
import tempfile

import pytest

SOURCE = Path("/app/streaming.cpp")
EXECUTABLE = Path("/app/CombinedStreamEngine")
OUTPUT = Path("/app/output.txt")
COMPILER_COMMAND = "g++ -std=c++20 -O2 -pthread"
PREFIX = (b"\x1b[0m", b"\x1b[32m", b"\x1b[36m", b"\x1b[1;33m", b"\x1b[1;35m")
SAMPLE_SOURCE = b"Adaptive streaming turns this short paragraph into several formatted word chunks for demonstration.\n"
SAMPLE_QUERIES = b"""# Number of queries
2

# Query 0: ID, then number of packets
0 2
0 5 1   # first word, word count, quality
3 5 4   # overlaps the first packet

# Query 1: starts with an empty cache, so it fetches quality 2
1 1
5 4 2
"""


def split_words(data):
    """Use the instruction's six ASCII whitespace bytes."""
    result = []
    start = None
    for pos, byte in enumerate(data + b" "):
        if byte in b" \t\r\n\f\v":
            if start is not None:
                result.append(data[start:pos])
                start = None
        elif start is None:
            start = pos
    return result


def escaped(word):
    for original, replacement in ((b"&", b"&amp;"), (b"<", b"&lt;"),
                                  (b">", b"&gt;"), (b'"', b"&quot;"),
                                  (b"'", b"&apos;")):
        word = word.replace(original, replacement)
    return word


def formatted(word, quality):
    return (PREFIX[quality] + f'<font quality="{quality}">'.encode()
            + escaped(word) + b"</font>\x1b[0m")


def parse_queries(data):
    lines = [line.split(b"#", 1)[0].split() for line in data.splitlines()]
    lines = [line for line in lines if line]
    count = int(lines[0][0])
    cursor = 1
    queries = []
    for _ in range(count):
        query_id, packet_count = map(int, lines[cursor])
        cursor += 1
        packets = []
        for _ in range(packet_count):
            packets.append(tuple(map(int, lines[cursor])))
            cursor += 1
        queries.append((query_id, packets))
    assert cursor == len(lines)
    return queries


def accesses(packets):
    result = []
    for time, duration, quality in packets:
        if duration == 0:
            continue
        for group in range(time // 5, (time + duration - 1) // 5 + 1):
            begin = max(time, group * 5)
            end = min(time + duration, (group + 1) * 5)
            result.append((group, quality, range(begin, end)))
    return result


def minimum_cost(trace, cost, displayed=None):
    """Solve weighted cache reuse as a seven-track minimum-cost flow."""
    count = len(trace)
    if not count:
        return 0
    slots = 7
    source, sink = 2 * count, 2 * count + 1
    graph = [[] for _ in range(sink + 1)]

    def add(start, end, capacity, price):
        forward = [end, len(graph[end]), capacity, price]
        backward = [start, len(graph[start]), 0, -price]
        graph[start].append(forward)
        graph[end].append(backward)
        return forward

    prices = [cost(group, requested) for group, requested, _ in trace]
    baseline = sum(prices)
    bonus = baseline + 1
    required_count = 0
    if displayed is not None:
        last = {}
        for index, (group, requested, _) in enumerate(trace):
            if group not in last and displayed[index] != requested:
                return None
            if group in last and displayed[index] != requested:
                if displayed[index] != displayed[last[group]]:
                    return None
                required_count += 1
            last[group] = index

    # A missed request edge would forgo more than all possible reuse savings.
    mandatory = baseline + required_count * bonus + 1
    add(source, 0, slots, 0)
    request_edges = []
    reuse_edges = []
    last = {}
    for index, (group, requested, _) in enumerate(trace):
        request_edges.append(add(2 * index, 2 * index + 1, 1, -mandatory))
        add(2 * index, 2 * index + 1, slots - 1, 0)
        if index + 1 < count:
            add(2 * index + 1, 2 * index + 2, slots, 0)
        if group in last:
            forbidden = displayed is not None and displayed[index] != displayed[last[group]]
            required = displayed is not None and displayed[index] != requested
            if not forbidden:
                edge = add(2 * last[group] + 1, 2 * index, 1,
                           -prices[index] - (bonus if required else 0))
                reuse_edges.append((edge, prices[index], required))
        last[group] = index
    add(2 * count - 1, sink, slots, 0)

    # Initial edges form a DAG. Their shortest paths initialise nonnegative
    # reduced costs for the residual-network Dijkstra searches.
    infinity = 10**100
    potential = [infinity] * len(graph)
    potential[source] = 0
    for node in [source, *range(2 * count), sink]:
        if potential[node] == infinity:
            continue
        for end, _, capacity, price in graph[node]:
            if capacity:
                potential[end] = min(potential[end], potential[node] + price)

    sent = 0
    while sent < slots:
        distance = [infinity] * len(graph)
        parent = [None] * len(graph)
        distance[source] = 0
        queue = [(0, source)]
        while queue:
            spent, node = heappop(queue)
            if spent != distance[node]:
                continue
            for edge_index, (end, _, capacity, price) in enumerate(graph[node]):
                if not capacity:
                    continue
                next_spent = spent + price + potential[node] - potential[end]
                if next_spent < distance[end]:
                    distance[end] = next_spent
                    parent[end] = (node, edge_index)
                    heappush(queue, (next_spent, end))
        if parent[sink] is None:
            return None
        for node, value in enumerate(distance):
            if value < infinity:
                potential[node] += value
        amount = slots - sent
        node = sink
        while node != source:
            start, edge_index = parent[node]
            amount = min(amount, graph[start][edge_index][2])
            node = start
        node = sink
        while node != source:
            start, edge_index = parent[node]
            edge = graph[start][edge_index]
            edge[2] -= amount
            graph[node][edge[1]][2] += amount
            node = start
        sent += amount

    if any(edge[2] for edge in request_edges):
        return None
    if any(required and edge[2] for edge, _, required in reuse_edges):
        return None
    return baseline - sum(price for edge, price, _ in reuse_edges if edge[2] == 0)


def verify_output(data, source, queries_text):
    words = split_words(source)
    queries = parse_queries(queries_text)
    assert data.endswith(b"\n"), "output must end with a newline"
    all_lines = data.split(b"\n")
    cursor = 0

    @lru_cache(maxsize=None)
    def cost(group, quality):
        block = words[group * 5:(group + 1) * 5]
        return len(b" ".join(formatted(word, quality) for word in block))

    for query_index, (query_id, packets) in enumerate(queries):
        lines = all_lines[cursor:cursor + 3]
        cursor += 3
        assert len(lines) == 3, "section needs a header, word line, and byte line"
        if query_index:
            assert all_lines[cursor - 4] == b"", "leave one blank line between sections"
        assert lines[0] == f"[Query{query_id}]".encode()
        assert lines[2].startswith(b"[NetworkBytes] ")
        amount = lines[2][len(b"[NetworkBytes] "):]
        assert amount.isdigit()
        reported = int(amount)
        trace = accesses(packets)
        observed = []
        position = 0
        word_number = 0
        for group, requested, indices in trace:
            qualities = []
            for index in indices:
                if word_number:
                    assert lines[1][position:position + 1] == b" ", "words need single-space separators"
                    position += 1
                matching = [q for q in range(5)
                            if lines[1].startswith(formatted(words[index], q), position)]
                assert len(matching) == 1, f"word {index} is incorrectly formatted"
                position += len(formatted(words[index], matching[0]))
                word_number += 1
                qualities.append(matching[0])
            assert len(set(qualities)) == 1, "one group access uses one held quality"
            observed.append(qualities[0])
        assert position == len(lines[1]), "unexpected words or bytes after selected words"

        minimum = minimum_cost(trace, cost)
        assert reported == minimum, f"query {query_id}: expected minimum {minimum}, got {reported}"

        styled_minimum = minimum_cost(trace, cost, observed)
        assert styled_minimum == minimum, (
            "rendered qualities do not admit an optimal seven-slot cache trace"
        )
        if query_index + 1 < len(queries):
            assert all_lines[cursor] == b"", "leave one blank line between sections"
            cursor += 1
    assert all_lines[cursor:] == [b""], "unexpected lines after final section"


def unprivileged():
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(38, 1, 0, 0, 0) != 0:  # PR_SET_NO_NEW_PRIVS
        raise OSError(ctypes.get_errno(), "PR_SET_NO_NEW_PRIVS failed")
    nobody = pwd.getpwnam("nobody")
    os.setgroups([])
    os.setgid(nobody.pw_gid)
    os.setuid(nobody.pw_uid)


@pytest.fixture(scope="session", autouse=True)
def protect_reward_directory():
    """Only the root verifier may access the reward path while agent code runs."""
    reward_dir = Path("/logs/verifier")
    assert reward_dir.stat().st_uid == 0
    reward_dir.chmod(0o700)


def kill_group(pid):
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run_program(program, source, queries):
    with tempfile.TemporaryDirectory(prefix="stream-case-") as directory:
        directory = Path(directory)
        directory.chmod(0o777)
        source_path = directory / "source.txt"
        query_path = directory / "queries.txt"
        output_path = directory / "output.txt"
        safe_program = directory / "engine"
        shutil.copyfile(program, safe_program)
        safe_program.chmod(0o755)
        source_path.write_bytes(source)
        query_path.write_bytes(queries)
        with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
            process = subprocess.Popen(
                [str(safe_program), str(source_path), str(query_path), str(output_path)],
                cwd=directory, stdout=stdout_file, stderr=stderr_file,
                start_new_session=True, preexec_fn=unprivileged,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            )
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                kill_group(process.pid)
                process.wait(timeout=5)
                pytest.fail("executable exceeded the 30-second run limit")
            finally:
                kill_group(process.pid)
            stderr_file.seek(0)
            stderr = stderr_file.read(4096)
            assert process.returncode == 0, stderr.decode(errors="replace")
        assert output_path.is_file(), "executable did not produce the requested output"
        return output_path.read_bytes()


@pytest.fixture(scope="module")
def compiled_program():
    """Build the submitted C++20 source for private-input checks."""
    assert SOURCE.is_file(), "missing /app/streaming.cpp"
    assert EXECUTABLE.is_file(), "missing /app/CombinedStreamEngine"
    with tempfile.TemporaryDirectory(prefix="stream-build-") as directory:
        Path(directory).chmod(0o755)
        executable = Path(directory) / "engine"
        result = subprocess.run(COMPILER_COMMAND.split() + [str(SOURCE), "-o", str(executable)],
                                capture_output=True, timeout=90,
                                check=False)
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        executable.chmod(0o755)
        yield executable


def test_public_sample_and_delivered_executable():
    """The binary and /app/output.txt obey /app/sample_source.txt and /app/sample_queries.txt."""
    assert OUTPUT.is_file(), "missing /app/output.txt"
    verify_output(OUTPUT.read_bytes(), SAMPLE_SOURCE, SAMPLE_QUERIES)
    verify_output(run_program(EXECUTABLE, SAMPLE_SOURCE, SAMPLE_QUERIES),
                  SAMPLE_SOURCE, SAMPLE_QUERIES)


def test_hidden_weighted_reuse_and_reset(compiled_program):
    """Both deliverables must optimize private group costs and reset the cache per query."""
    words = [f"g{i // 5}_{i}" for i in range(43)]
    words[0] = "<&'\"é" * 12
    words[21] = "&" * 25
    source = (" \t".join(words) + "\n").encode()
    groups = [0, 1, 2, 3, 4, 5, 0, 6, 1, 7, 0, 8, 4, 2, 6, 0, 5, 8, 3]
    packets = [f"{g * 5} 1 {i % 5}" for i, g in enumerate(groups)]
    query = ("2\n007 " + str(len(packets) + 1) + "\n" + "\n".join(packets)
             + "\n43 0 4 # empty packet\n8 2\n0 1 4\n40 3 1\n").encode()
    verify_output(run_program(compiled_program, source, query), source, query)
    verify_output(run_program(EXECUTABLE, source, query), source, query)


def test_hidden_cost_sensitive_eviction(compiled_program):
    """Both deliverables beat next-use eviction when the farthest group is costly."""
    words = [f"w{i}" for i in range(40)]
    words[0] = "expensive&<>" * 18
    source = (" ".join(words) + "\n").encode()
    # At the eighth access, all seven held groups recur. Furthest-next-use
    # evicts costly group 0; the optimum refetches one of the cheap groups.
    groups = [0, 1, 2, 3, 4, 5, 6, 7, 6, 5, 4, 3, 2, 1, 0]
    packets = "\n".join(f"{group * 5} 1 0" for group in groups)
    query = f"1\n19 {len(groups)}\n{packets}\n".encode()
    verify_output(run_program(compiled_program, source, query), source, query)
    verify_output(run_program(EXECUTABLE, source, query), source, query)


def test_hidden_large_weighted_sequence(compiled_program):
    """Both deliverables optimize many revisits across 30 groups and seven slots."""
    words = [f"group{i // 5}_word{i}" for i in range(150)]
    for group in (0, 7, 13, 23):
        words[group * 5] = "<&é>" * (16 + group % 4 * 5)
    source = (" \t".join(words) + "\n").encode()
    groups = list(range(30))
    groups.extend((index * 17 + index * index * 7) % 30 for index in range(70))
    packets = []
    for index, group in enumerate(groups):
        span = index % 3 == 0 and group < 29
        time = group * 5 + (4 if span else 0)
        packets.append(f"{time} {2 if span else 1} {(index * 3 + group) % 5}")
    query = ("1\n27 100\n" + "\n".join(packets) + "\n").encode()
    verify_output(run_program(compiled_program, source, query), source, query)
    verify_output(run_program(EXECUTABLE, source, query), source, query)


def test_hidden_packet_spans_and_empty_query(compiled_program):
    """Packet order, partial groups, empty selections, escaping, and query IDs work on private input."""
    words = [f"w{i}" for i in range(28)]
    words[3], words[27] = "<xml>", "o'clock&"
    source = ("\r\n".join(words) + "\n").encode()
    query = b"""3
10 5
3 12 3
0 0 4
20 8 1
5 16 2
24 4 0
11 0
12 2
27 1 4
2 3 1
"""
    verify_output(run_program(compiled_program, source, query), source, query)
