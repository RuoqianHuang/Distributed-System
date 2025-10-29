

server: ./cmd/server/main.go
	go build -o ./bin/server ./cmd/server

client: ./cmd/client/main.go
	go build -o ./bin/client ./cmd/client
	
clean:
	rm ./bin/client
	rm ./bin/server