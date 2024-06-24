run:
	go run cmd/main.go

proto:
	rm -f protogen/*.go
	protoc --proto_path=proto --go_out=protogen --go_opt=paths=source_relative \
	--go-grpc_out=protogen --go-grpc_opt=paths=source_relative \
	./proto/*.proto 

.PHONY: run proto