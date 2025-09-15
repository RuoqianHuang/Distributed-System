# Machine Programming 1 – Distributed Log Querier

## How to build server and client binary
````
# Build the client
go build -o bin/client ./cmd/client

# Build the server
go build -o bin/server ./cmd/server
````

## How to deploy

### Prerequisite
1. All remote servers installed with Python and ssh server.
2. Setup ssh key and passwordless sudo on remote servers.
3. Install ansible-core

### Steps
1. Build server binary with ``go build server.go``
2. Deploy server binary with ``ansible-playbook -i inventory.ini install-playbook.yml``
3. Run client: ``go run cmd/client/main.go`` (with any desired pattern)
4. To uninstall server binary, run ``ansible-playbook -i inventory.ini uninstall-playbook.yml``

### Other commands
1. ping test: ``ansible -i inventory.ini my_servers -m ping``
2. server log: ``journalctl -u MP1_server.service -n 50``

## Client usage

````
./bin/client [-f <file_pattern>] [grep_options] <grep_pattern>
````
- ``-f <file_pattern>`` (Optional)
    - Specifies the glob pattern for the log files to search.
    - **Default**: If this flag is not set, the client defaults to search ``/cs425/mp1/vm*.log``.

- ``[grep_options]`` (Optional)
    - All standard ``grep`` flag are supported and are passed directly to the grep command.

- ``<grep_pattern>`` (Required)
    - The pattern to search for within the specified files.
    

## How to run tests

````
go test internal/caller/mp1_test.go -v
````