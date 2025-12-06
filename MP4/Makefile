

rainstorm: ./cmd/rainstorm/main.go
	go build -o ./bin/rainstorm ./cmd/rainstorm

worker: ./cmd/worker/main.go
	go build -o ./bin/worker ./cmd/worker

monitor: ./cmd/monitor/main.go
	go build -o ./bin/monitor ./cmd/monitor

clean:
	rm ./bin/rainstorm
	rm ./bin/worker