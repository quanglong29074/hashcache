#!/bin/bash
# Test reading keys

KEY=${1:-test_key_1}
echo "Reading key: $KEY"
curl http://localhost:3000/$KEY
echo ""
