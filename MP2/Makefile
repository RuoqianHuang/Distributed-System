

server: ./cmd/server/main.go
	go build -o ./bin/server ./cmd/server

client: ./cmd/client/main.go
	go build -o ./bin/client ./cmd/client

pltoa: ./cmd/plota/main.go
	go build -o ./bin/plota ./cmd/plota

plotb: ./cmd/plotb/main.go
	go build -o ./bin/plotb ./cmd/plotb
	
plotc: ./cmd/plotc/main.go
	go build -o ./bin/plotc ./cmd/plotc
	
clean:
	rm ./bin/client
	rm ./bin/server
	rm ./bin/plota
	rm ./bin/plotb
	rm ./bin/plotc