# Makefile for Operator Executables
# Builds all operator executables and installs them to /cs425/mp4/

OPERATORS = grep_op replace_op count_op count_by_column_op extract_fields_op
INSTALL_DIR = /cs425/mp4

.PHONY: all clean install test

all: $(OPERATORS)

grep_op: grep_op.go
	go build -o grep_op grep_op.go

replace_op: replace_op.go
	go build -o replace_op replace_op.go

count_op: count_op.go
	go build -o count_op count_op.go

count_by_column_op: count_by_column_op.go
	go build -o count_by_column_op count_by_column_op.go

extract_fields_op: extract_fields_op.go
	go build -o extract_fields_op extract_fields_op.go

install: all
	@echo "Installing operators to $(INSTALL_DIR)..."
	@mkdir -p $(INSTALL_DIR)
	@cp $(OPERATORS) $(INSTALL_DIR)/
	@chmod +x $(INSTALL_DIR)/*
	@echo "Operators installed successfully!"

clean:
	rm -f $(OPERATORS)

test: all
	@echo "Testing grep_op..."
	@(echo "key: file1:1"; echo "value: hello world") | ./grep_op "hello" | head -1 | grep -q "forward" && echo "✓ grep_op test passed" || echo "✗ grep_op test failed"
	
	@echo "Testing replace_op..."
	@(echo "key: file1:1"; echo "value: hello world") | ./replace_op "hello" "hi" | grep -q "value: hi world" && echo "✓ replace_op test passed" || echo "✗ replace_op test failed"
	
	@echo "Testing count_op..."
	@(echo "key: word1"; echo "value: 1"; echo "key: word1"; echo "value: 1") | ./count_op | tail -1 | grep -q "value: 2" && echo "✓ count_op test passed" || echo "✗ count_op test failed"

