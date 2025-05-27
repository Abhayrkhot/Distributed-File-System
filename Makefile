SHELL := /bin/bash
BIN   := bin
PKG   := ./...
KIND_CLUSTER ?= dfs
IMAGE ?= dfs:dev

.PHONY: build test test-race integration proto deps-up deps-down bench-throughput bench-failover \
        docker kind-up kind-deploy kind-smoke kind-down k8s-render lint clean

build:
	go build -o $(BIN)/ ./cmd/...

proto:
	PATH=$$HOME/go/bin:$$PATH protoc -I proto --go_out=gen --go_opt=paths=source_relative \
	  --go-grpc_out=gen --go-grpc_opt=paths=source_relative proto/dfs/v1/dfs.proto

# Unit + integration tests against an in-process cluster (in-memory metastore,
# directory-backed S3 tier). No external services needed.
test:
	go test -count=1 $(PKG)

test-race:
	go test -race -count=1 $(PKG)

# Same tests, but against real Cassandra and MinIO (scripts/dev-deps.sh up).
integration: deps-up
	eval "$$(scripts/dev-deps.sh env)" && go test -count=1 -v ./internal/metastore ./internal/blob ./test/integration

deps-up:
	scripts/dev-deps.sh up

deps-down:
	scripts/dev-deps.sh down

bench-throughput: build deps-up
	eval "$$(scripts/dev-deps.sh env)" && $(BIN)/dfs-bench throughput --storage 50 --clients 32 --total 1536MiB --label 50nodes

bench-failover: build
	$(BIN)/dfs-bench failover --trials 30 --timing fast --raft-store wal
	$(BIN)/dfs-bench failover --trials 30 --timing fast --raft-store wal --mode freeze

docker:
	docker build -t $(IMAGE) .

k8s-render:
	kubectl kustomize deploy/k8s

kind-up:
	kind create cluster --name $(KIND_CLUSTER) --config deploy/kind/cluster.yaml

kind-deploy: docker
	kind load docker-image $(IMAGE) --name $(KIND_CLUSTER)
	kubectl --context kind-$(KIND_CLUSTER) apply -k deploy/k8s
	kubectl --context kind-$(KIND_CLUSTER) -n dfs rollout status statefulset/dfs-meta --timeout=300s
	kubectl --context kind-$(KIND_CLUSTER) -n dfs rollout status statefulset/dfs-storage --timeout=300s

kind-smoke:
	kubectl --context kind-$(KIND_CLUSTER) -n dfs delete job dfs-smoke --ignore-not-found
	kubectl --context kind-$(KIND_CLUSTER) -n dfs apply -f deploy/k8s/smoke-job.yaml
	kubectl --context kind-$(KIND_CLUSTER) -n dfs wait --for=condition=complete job/dfs-smoke --timeout=600s
	kubectl --context kind-$(KIND_CLUSTER) -n dfs logs job/dfs-smoke

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

lint:
	go vet $(PKG)
	gofmt -l . | (! grep .)

clean:
	rm -rf $(BIN)
