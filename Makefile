.PHONY: build test vet generate integration
build:
	go build -trimpath -o bin/usp-controller ./cmd/usp-controller
test:
	go test -race ./...
vet:
	go vet ./...
generate:
	protoc --proto_path=proto --go_out=. --go_opt=module=example.com/usp-controller --go_opt=Musp-msg-1-5.proto=example.com/usp-controller/internal/pb/usp --go_opt=Musp-record-1-5.proto=example.com/usp-controller/internal/pb/record proto/usp-msg-1-5.proto proto/usp-record-1-5.proto
integration:
	test -n "$(USP_TEST_MQTT)"
	go test -race -count=1 ./internal/mqtt -v
