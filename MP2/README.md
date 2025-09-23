# Machine Programming 2 – Failure Detector (it detects me!)

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
1. Build the server binary.
2. Deploy server binary with ``ansible-playbook -i inventory.ini install-playbook.yml``
3. To uninstall server binary, run ``ansible-playbook -i inventory.ini uninstall-playbook.yml``


### How to query membership and status

You can use the client binary to make query to any node with rpc. Here's an example
````
# To query membership
./client membership fa25-cs425-b604.cs.illinois.edu

# To query status
./bin/client status fa25-cs425-b604.cs.illinois.edu

````


### Other commands
1. ping test: ``ansible -i inventory.ini my_servers -m ping``
2. server log: ``journalctl -u MP1_server.service -n 50``
3. server log: ``tail -n 40 /cs425/mp2/vm%2d.log``