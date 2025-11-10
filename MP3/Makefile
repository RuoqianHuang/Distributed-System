

server: ./cmd/server/main.go
	go build -o ./bin/server ./cmd/server

client: ./cmd/client/main.go
	go build -o ./bin/client ./cmd/client

interactive: ./cmd/interactive/main.go
	go build -o ./bin/interactive ./cmd/interactive

plota: ./cmd/plota/main.go
	go build -o ./bin/plota ./cmd/plota

plotb: ./cmd/plotb/main.go
	go build -o ./bin/plotb ./cmd/plotb

plotm: ./cmd/plotm/main.go
	go build -o ./bin/plotm ./cmd/plotm

plott: ./cmd/plott/main.go
	go build -o ./bin/plott ./cmd/plott

clean:
	rm ./bin/client
	rm ./bin/server
	rm ./bin/interactive