# Distributed File System in Go

A lightweight and modular distributed file system implemented in Go, featuring peer-to-peer (P2P) communication, encrypted storage, and fault-tolerant data replication. This project is ideal for understanding how decentralized storage networks operate.

---

## 📁 Project Structure

```
.
├── main.go                # Entry point of the application
├── server.go              # HTTP server for file operations
├── store.go               # Local storage management
├── crypto.go              # Encryption/decryption logic
├── Makefile               # Build automation
├── go.mod / go.sum        # Go module files
├── p2p/
│   ├── transport.go       # Abstract transport interface
│   ├── tcp_transport.go   # TCP-based P2P transport layer
│   ├── encoding.go        # Message encoding utilities
│   ├── handshake.go       # Peer handshake protocol
│   └── message.go         # Message types for P2P communication
├── *_test.go              # Unit tests for core modules
```

---

## 🚀 Features

- **Peer-to-Peer Transport** via TCP
- **Modular P2P Framework** for message handling and handshake
- **Secure File Storage** using cryptographic techniques
- **Fault Tolerance** and potential for data replication
- **Unit Tested** modules to ensure correctness

---

## ⚙️ Getting Started

### Prerequisites

- [Go 1.20+](https://golang.org/dl/)

### Installation

```bash
git clone https://github.com/Abhayrkhot/Distributed-File-System.git
cd distributedfilesystemgo
make build
```

### Run a Node

```bash
go run main.go
```

You can launch multiple nodes with different ports to simulate a distributed environment.

---

## 🧪 Running Tests

```bash
go test ./...
```

---

## 📡 P2P Protocol

- **Handshake**: Initial node authentication
- **Message Types**: File push/pull requests, replication instructions, etc.
- **Transport Layer**: TCP-based, but can be extended for WebRTC, QUIC, etc.

---

## 🔒 Cryptography

Implemented encryption/decryption for file blocks to ensure secure storage and transfer.

---

## 🛠️ TODO

- Add REST API for file uploads/downloads
- Implement data replication strategies
- Improve peer discovery (e.g., DHT)
