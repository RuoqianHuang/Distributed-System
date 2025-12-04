#!/bin/bash
# Test script for operator executables using correct protocol

echo "=========================================="
echo "Testing Operator Executables (Protocol)"
echo "=========================================="
echo ""

# Test grep_op
echo "[1/3] Testing grep_op (Filter)..."
echo "Input:"
echo "  key: file1:1"
echo "  value: hello world"
echo "Pattern: hello"
echo "Expected: forward with same key/value"
echo "Actual:"
(echo "key: file1:1"; echo "value: hello world") | ./grep_op "hello"
echo ""

echo "Input:"
echo "  key: file1:2"
echo "  value: goodbye"
echo "Pattern: hello"
echo "Expected: filter"
echo "Actual:"
(echo "key: file1:2"; echo "value: goodbye") | ./grep_op "hello"
echo ""

# Test replace_op
echo "[2/3] Testing replace_op (Transform)..."
echo "Input:"
echo "  key: file1:1"
echo "  value: hello world"
echo "Replace: hello -> hi"
echo "Expected: forward with modified value"
echo "Actual:"
(echo "key: file1:1"; echo "value: hello world") | ./replace_op "hello" "hi"
echo ""

# Test count_op (persistent process simulation)
echo "[3/3] Testing count_op (Aggregate) - Persistent Process..."
echo "Input (multiple tuples to same process):"
echo "  key: word1, value: 1"
echo "  key: word2, value: 1"
echo "  key: word1, value: 1"
echo "Expected: word1 count should increase (1, then 2)"
echo "Actual:"
(echo "key: word1"; echo "value: 1"; echo "key: word2"; echo "value: 1"; echo "key: word1"; echo "value: 1") | ./count_op
echo ""

echo "=========================================="
echo "Testing Complete"
echo "=========================================="
echo ""
echo "Note: Operators use protocol:"
echo "  Input:  key: <key>\\nvalue: <value>\\n"
echo "  Output: filter OR forward\\nkey: <key>\\nvalue: <value>\\n"

