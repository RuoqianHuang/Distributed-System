#!/bin/bash
# Test script for operator executables using correct protocol

echo "=========================================="
echo "Testing Operator Executables (Protocol)"
echo "=========================================="
echo ""

# Test grep_op
echo "[1/5] Testing grep_op (Filter)..."
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
echo "[2/5] Testing replace_op (Transform)..."
echo "Input:"
echo "  key: file1:1"
echo "  value: hello world"
echo "Replace: hello -> hi"
echo "Expected: forward with modified value"
echo "Actual:"
(echo "key: file1:1"; echo "value: hello world") | ./replace_op "hello" "hi"
echo ""

# Test count_op (persistent process simulation)
echo "[3/5] Testing count_op (Aggregate) - Persistent Process..."
echo "Input (multiple tuples to same process):"
echo "  key: word1, value: 1"
echo "  key: word2, value: 1"
echo "  key: word1, value: 1"
echo "Expected: word1 count should increase (1, then 2)"
echo "Actual:"
(echo "key: word1"; echo "value: 1"; echo "key: word2"; echo "value: 1"; echo "key: word1"; echo "value: 1") | ./count_op
echo ""

# Test count_by_column_op
echo "[4/5] Testing count_by_column_op (Aggregate) - Count by Column..."
echo "Input (multiple tuples to same process):"
echo "  key: line1, value: a,apple,red"
echo "  key: line2, value: b,banana,yellow"
echo "  key: line3, value: c,apple,green"
echo "  key: line4, value: d,apple,blue"
echo "N = 2 (2nd column)"
echo "Expected: apple count should increase (1, 2, 3)"
echo "Actual:"
(echo "key: line1"; echo "value: a,apple,red"; echo "key: line2"; echo "value: b,banana,yellow"; echo "key: line3"; echo "value: c,apple,green"; echo "key: line4"; echo "value: d,apple,blue") | ./count_by_column_op 2
echo ""

echo "Test: Missing column (N=5, only 3 columns)"
echo "Input: key: line1, value: a,b,c"
echo "Expected: key should be empty string, count = 1"
echo "Actual:"
(echo "key: line1"; echo "value: a,b,c") | ./count_by_column_op 5
echo ""

echo "Test: Empty field"
echo "Input: key: line1, value: a,,c"
echo "Expected: key should be empty string, count = 1"
echo "Actual:"
(echo "key: line1"; echo "value: a,,c") | ./count_by_column_op 2
echo ""

echo "Test: Single space field"
echo "Input: key: line1, value: a, ,c"
echo "Expected: key should be single space ' ', count = 1"
echo "Actual:"
(echo "key: line1"; echo "value: a, ,c") | ./count_by_column_op 2
echo ""

# Test extract_fields_op
echo "[5/5] Testing extract_fields_op (Transform) - Extract Fields..."
echo "Input:"
echo "  key: line1"
echo "  value: line1,apple,red,large,sweet"
echo "N = 3 (first 3 fields)"
echo "Expected: forward with first 3 fields"
echo "Actual:"
(echo "key: line1"; echo "value: line1,apple,red,large,sweet") | ./extract_fields_op 3
echo ""

echo "Test: Fewer fields than requested"
echo "Input: key: line1, value: a,b"
echo "N = 3 (requesting 3 fields, but only 2 available)"
echo "Expected: forward with all available fields (a,b)"
echo "Actual:"
(echo "key: line1"; echo "value: a,b") | ./extract_fields_op 3
echo ""

echo "=========================================="
echo "Testing Complete"
echo "=========================================="
echo ""
echo "Note: Operators use protocol:"
echo "  Input:  key: <key>\\nvalue: <value>\\n"
echo "  Output: filter OR forward\\nkey: <key>\\nvalue: <value>\\n"

