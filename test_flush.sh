#!/bin/bash
# Trigger manual flush

echo "Triggering manual flush..."
response=$(curl -s -X POST http://localhost:3000/flush)
echo "$response"
echo ""
echo "Check manifest: curl http://localhost:3000/manifest | jq"
