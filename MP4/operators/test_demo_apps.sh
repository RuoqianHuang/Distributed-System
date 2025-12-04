#!/bin/bash
# Test script for demo applications

echo "=========================================="
echo "Testing Demo Applications"
echo "=========================================="
echo ""

# Application 1: Filter & Count
echo "=== Application 1: Filter & Count ==="
echo ""
echo "Stage 1 (Filter): Accept lines containing pattern 'test'"
echo "Stage 2 (AggregateByKey): Count by 2nd column"
echo ""

echo "Input data:"
echo "  line1,apple,red"
echo "  line2,banana,yellow"
echo "  line3,apple,green"
echo "  line4,test,data"
echo "  line5,apple,blue"
echo ""

echo "After Stage 1 (Filter with pattern 'test'):"
echo "  line4,test,data"
echo ""

echo "After Stage 2 (Count by 2nd column):"
echo "Input: line4,test,data (2nd column = 'test')"
(echo "key: line4"; echo "value: line4,test,data") | ./count_by_column_op 2
echo ""

# Application 2: Filter & Transform
echo "=== Application 2: Filter & Transform ==="
echo ""
echo "Stage 1 (Filter): Accept lines containing pattern 'apple'"
echo "Stage 2 (Transform): Extract first 3 fields"
echo ""

echo "Input data:"
echo "  line1,apple,red,large,sweet"
echo "  line2,banana,yellow,medium,sweet"
echo "  line3,apple,green,small,sour"
echo ""

echo "After Stage 1 (Filter with pattern 'apple'):"
echo "  line1,apple,red,large,sweet"
echo "  line3,apple,green,small,sour"
echo ""

echo "After Stage 2 (Extract first 3 fields):"
echo "Input: line1,apple,red,large,sweet"
(echo "key: line1"; echo "value: line1,apple,red,large,sweet") | ./extract_fields_op 3
echo ""
echo "Input: line3,apple,green,small,sour"
(echo "key: line3"; echo "value: line3,apple,green,small,sour") | ./extract_fields_op 3
echo ""

echo "=========================================="
echo "Testing Edge Cases"
echo "=========================================="
echo ""

# Test count_by_column_op with missing data
echo "Test: Missing column (N=5, only 3 columns)"
(echo "key: line1"; echo "value: a,b,c") | ./count_by_column_op 5
echo ""

# Test count_by_column_op with empty string
echo "Test: Empty field (empty string as key)"
(echo "key: line1"; echo "value: a,,c") | ./count_by_column_op 2
echo ""

# Test count_by_column_op with single space
echo "Test: Single space field"
(echo "key: line1"; echo "value: a, ,c") | ./count_by_column_op 2
echo ""

# Test extract_fields_op with fewer fields
echo "Test: Extract 3 fields from line with only 2 fields"
(echo "key: line1"; echo "value: a,b") | ./extract_fields_op 3
echo ""

echo "=========================================="
echo "Testing Complete"
echo "=========================================="

