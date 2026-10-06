#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace std;

struct Packet { size_t time, duration; int quality; };
struct Query { uint64_t id; vector<Packet> packets; };
struct Access { int group, quality; size_t begin, end; };

struct Edge { int to, reverse, capacity; int64_t cost; };

class FlowNetwork {
public:
    explicit FlowNetwork(int count) : edges(count) {}

    pair<int, int> add(int from, int to, int capacity, int64_t cost) {
        int index = static_cast<int>(edges[from].size());
        int reverse = static_cast<int>(edges[to].size());
        edges[from].push_back({to, reverse, capacity, cost});
        edges[to].push_back({from, index, 0, -cost});
        return {from, index};
    }

    void send(int source, int sink, int amount) {
        const int64_t inf = numeric_limits<int64_t>::max() / 4;
        vector<int64_t> potential(edges.size(), inf);
        potential[source] = 0;
        vector<int> order{source};
        for (int node = 0; node < source; ++node) order.push_back(node);
        order.push_back(sink);
        // Original edges go forward in time, so this is a DAG shortest path.
        for (int node : order) {
            if (potential[node] == inf) continue;
            for (const Edge& edge : edges[node]) {
                if (edge.capacity && potential[edge.to] > potential[node] + edge.cost)
                    potential[edge.to] = potential[node] + edge.cost;
            }
        }
        while (amount > 0) {
            vector<int64_t> distance(edges.size(), inf);
            vector<pair<int, int>> parent(edges.size(), {-1, -1});
            priority_queue<pair<int64_t, int>, vector<pair<int64_t, int>>,
                           greater<pair<int64_t, int>>> queue;
            distance[source] = 0;
            queue.push({0, source});
            while (!queue.empty()) {
                auto [spent, node] = queue.top();
                queue.pop();
                if (spent != distance[node]) continue;
                for (int index = 0; index < static_cast<int>(edges[node].size()); ++index) {
                    const Edge& edge = edges[node][index];
                    if (!edge.capacity) continue;
                    int64_t next = spent + edge.cost + potential[node] - potential[edge.to];
                    if (next < distance[edge.to]) {
                        distance[edge.to] = next;
                        parent[edge.to] = {node, index};
                        queue.push({next, edge.to});
                    }
                }
            }
            if (parent[sink].first < 0) throw runtime_error("cache flow is infeasible");
            for (size_t node = 0; node < edges.size(); ++node)
                if (distance[node] < inf) potential[node] += distance[node];
            int pushed = amount;
            for (int node = sink; node != source; node = parent[node].first) {
                auto [from, index] = parent[node];
                pushed = min(pushed, edges[from][index].capacity);
            }
            for (int node = sink; node != source; node = parent[node].first) {
                auto [from, index] = parent[node];
                Edge& edge = edges[from][index];
                edge.capacity -= pushed;
                edges[node][edge.reverse].capacity += pushed;
            }
            amount -= pushed;
        }
    }

    bool used(pair<int, int> reference) const {
        return edges[reference.first][reference.second].capacity == 0;
    }

private:
    vector<vector<Edge>> edges;
};

static bool white(unsigned char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

static vector<string> split_words(const string& s) {
    vector<string> words;
    for (size_t i = 0; i < s.size();) {
        while (i < s.size() && white(static_cast<unsigned char>(s[i]))) ++i;
        size_t start = i;
        while (i < s.size() && !white(static_cast<unsigned char>(s[i]))) ++i;
        if (start < i) words.push_back(s.substr(start, i - start));
    }
    return words;
}

static string read_file(const string& path) {
    ifstream file(path, ios::binary);
    if (!file) throw runtime_error("cannot read " + path);
    return string(istreambuf_iterator<char>(file), {});
}

static vector<vector<string>> records(const string& path) {
    istringstream input(read_file(path));
    vector<vector<string>> result;
    string line;
    while (getline(input, line)) {
        if (auto comment = line.find('#'); comment != string::npos) line.erase(comment);
        vector<string> fields = split_words(line);
        if (!fields.empty()) result.push_back(move(fields));
    }
    return result;
}

static uint64_t number(const string& value) {
    size_t pos = 0;
    uint64_t n = stoull(value, &pos);
    if (pos != value.size()) throw runtime_error("invalid number");
    return n;
}

static vector<Query> parse_queries(const string& path) {
    auto lines = records(path);
    if (lines.empty() || lines[0].size() != 1) throw runtime_error("missing query count");
    size_t count = number(lines[0][0]), at = 1;
    vector<Query> queries;
    for (size_t i = 0; i < count; ++i) {
        if (at == lines.size() || lines[at].size() != 2) throw runtime_error("bad query header");
        Query q{number(lines[at][0]), {}};
        size_t packets = number(lines[at++][1]);
        for (size_t j = 0; j < packets; ++j) {
            if (at == lines.size() || lines[at].size() != 3) throw runtime_error("bad packet");
            auto& f = lines[at++];
            q.packets.push_back({static_cast<size_t>(number(f[0])),
                                 static_cast<size_t>(number(f[1])),
                                 static_cast<int>(number(f[2]))});
            if (q.packets.back().quality > 4) throw runtime_error("bad quality");
        }
        queries.push_back(move(q));
    }
    if (at != lines.size()) throw runtime_error("extra query data");
    return queries;
}

static string escape_word(const string& word) {
    string out;
    for (char c : word) {
        switch (c) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            case '"': out += "&quot;"; break;
            case '\'': out += "&apos;"; break;
            default: out += c;
        }
    }
    return out;
}

static string render(const string& word, int q) {
    static const array<string, 5> prefix = {"\x1b[0m", "\x1b[32m", "\x1b[36m", "\x1b[1;33m", "\x1b[1;35m"};
    return prefix.at(q) + "<font quality=\"" + to_string(q) + "\">" +
           escape_word(word) + "</font>\x1b[0m";
}

static uint64_t group_cost(const vector<string>& words, int group, int q) {
    size_t begin = static_cast<size_t>(group) * 5;
    size_t end = min(begin + 5, words.size());
    uint64_t cost = end - begin - 1;
    for (size_t i = begin; i < end; ++i) cost += render(words[i], q).size();
    return cost;
}

static vector<Access> accesses(const Query& q, size_t word_count) {
    vector<Access> result;
    for (const auto& p : q.packets) {
        if (p.time > word_count || p.duration > word_count - p.time) throw runtime_error("range outside source");
        size_t end = p.time + p.duration;
        if (p.duration == 0) continue;
        for (size_t g = p.time / 5; g <= (end - 1) / 5; ++g) {
            result.push_back({static_cast<int>(g), p.quality,
                              max(p.time, g * 5), min(end, (g + 1) * 5)});
        }
    }
    return result;
}

static pair<string, uint64_t> solve_query(const Query& q, const vector<string>& words) {
    auto trace = accesses(q, words.size());
    if (trace.empty()) return {"", 0};
    constexpr int slots = 7;
    int count = static_cast<int>(trace.size());
    int source = 2 * count, sink = source + 1;
    FlowNetwork network(sink + 1);
    vector<int64_t> costs(count);
    int64_t baseline = 0;
    for (int t = 0; t < count; ++t) {
        costs[t] = static_cast<int64_t>(group_cost(words, trace[t].group, trace[t].quality));
        baseline += costs[t];
    }
    int64_t mandatory_bonus = baseline + 1;
    network.add(source, 0, slots, 0);
    vector<pair<int, int>> requests(count), reuse(count, {-1, -1});
    map<int, int> previous;
    for (int t = 0; t < count; ++t) {
        requests[t] = network.add(2 * t, 2 * t + 1, 1, -mandatory_bonus);
        network.add(2 * t, 2 * t + 1, slots - 1, 0);
        if (t + 1 < count) network.add(2 * t + 1, 2 * t + 2, slots, 0);
        auto it = previous.find(trace[t].group);
        if (it != previous.end())
            reuse[t] = network.add(2 * it->second + 1, 2 * t, 1, -costs[t]);
        previous[trace[t].group] = t;
    }
    network.add(2 * count - 1, sink, slots, 0);
    network.send(source, sink, slots);
    int64_t minimum = baseline;
    vector<bool> hit(count, false);
    for (int t = 0; t < count; ++t) {
        if (!network.used(requests[t])) throw runtime_error("request was not covered");
        if (reuse[t].first >= 0 && network.used(reuse[t])) {
            hit[t] = true;
            minimum -= costs[t];
        }
    }
    map<int, int> held_quality;
    ostringstream output;
    bool first = true;
    for (int t = 0; t < count; ++t) {
        const auto& a = trace[t];
        if (!hit[t])
            held_quality[a.group] = a.quality;
        int quality = held_quality.at(a.group);
        for (size_t i = a.begin; i < a.end; ++i) {
            if (!first) output << ' ';
            output << render(words[i], quality);
            first = false;
        }
    }
    return {output.str(), minimum};
}

int main(int argc, char** argv) {
    try {
        if (argc != 4) throw runtime_error("expected source, queries, output paths");
        auto words = split_words(read_file(argv[1]));
        auto queries = parse_queries(argv[2]);
        ofstream output(argv[3], ios::binary);
        if (!output) throw runtime_error("cannot write output");
        for (size_t i = 0; i < queries.size(); ++i) {
            auto [text, cost] = solve_query(queries[i], words);
            if (i) output << '\n';
            output << "[Query" << queries[i].id << "]\n" << text
                   << "\n[NetworkBytes] " << cost << '\n';
        }
    } catch (const exception& e) {
        cerr << e.what() << '\n';
        return 1;
    }
}
