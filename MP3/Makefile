

server: ./cmd/server/main.go
	go build -o ./bin/server ./cmd/server

client: ./cmd/client/main.go
	go build -o ./bin/client ./cmd/client

interactive: ./cmd/interactive/main.go
	go build -o ./bin/interactive ./cmd/interactive
	
clean:
	rm ./bin/client
	rm ./bin/server
	rm ./bin/interactive