#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <algorithm>
#include <arpa/inet.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <cstring>
#include "json.hpp"

using json = nlohmann::json;
using namespace std;

#define SERVER_PORT 3000
#define SERVER_IP "127.0.0.1"

struct Packet {
    string symbol;
    char side;
    int quantity;
    int price;
    int sequence;

    json to_json() const {
        return {
            {"symbol", symbol},
            {"side", string(1, side)},
            {"quantity", quantity},
            {"price", price},
            {"sequence", sequence}
        };
    }
};

int connect_to_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr);

    if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        exit(EXIT_FAILURE);
    }

    return sock;
}

void send_call_type(int sock, uint8_t type, uint8_t seq = 0) {
    uint8_t buffer[2] = {type, seq};
    send(sock, buffer, 2, 0);
}

Packet parse_packet(const uint8_t* buffer) {
    Packet p;
    p.symbol = string(reinterpret_cast<const char*>(buffer), 4);
    p.side = buffer[4];
    p.quantity = ntohl(*reinterpret_cast<const int32_t*>(buffer + 5));
    p.price = ntohl(*reinterpret_cast<const int32_t*>(buffer + 9));
    p.sequence = ntohl(*reinterpret_cast<const int32_t*>(buffer + 13));
    return p;
}

vector<Packet> receive_packets(int sock, set<int>& received_seqs) {
    vector<Packet> packets;
    uint8_t buffer[17];

    while (true) {
        ssize_t bytes = recv(sock, buffer, 17, MSG_WAITALL);
        if (bytes <= 0) break;
        Packet p = parse_packet(buffer);
        packets.push_back(p);
        received_seqs.insert(p.sequence);
    }

    return packets;
}

Packet request_resend(uint8_t seq) {
    int sock = connect_to_server();
    send_call_type(sock, 2, seq);

    uint8_t buffer[17];
    recv(sock, buffer, 17, MSG_WAITALL);
    close(sock);
    return parse_packet(buffer);
}

int main() {
    
    int sock = connect_to_server();
    send_call_type(sock, 1);

    set<int> received_seqs;
    vector<Packet> all_packets = receive_packets(sock, received_seqs);
    close(sock);

    int max_seq = -1;
    for (auto& p : all_packets) {
        max_seq = max(max_seq, p.sequence);
    }

    for (int i = 1; i <= max_seq; ++i) {
        if (!received_seqs.count(i)) {
            all_packets.push_back(request_resend(i));
        }
    }

    sort(all_packets.begin(), all_packets.end(), [](const Packet& a, const Packet& b) {
        return a.sequence < b.sequence;
    });

    json output = json::array();
    for (auto& p : all_packets) {
        output.push_back(p.to_json());
    }

    ofstream out("output.json");
    out << output.dump(4) << endl;
    out.close();

    cout << max_seq << " details fetched successfully";
    return 0;
}
