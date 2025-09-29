#!/bin/bash
# Insert test data

COUNT=${1:-100}
echo "Inserting $COUNT keys..."

for i in $(seq 1 $COUNT); do
  curl -s -X PUT http://localhost:3000/test_key_$i \
    --data "test_value_$i_$(date +%s)" > /dev/null
  
  if (( i % 10 == 0 )); then
    echo "  Inserted $i/$COUNT keys..."
  fi
done

echo "✅ Inserted $COUNT keys"
echo ""
echo "Check stats: curl http://localhost:3000/stats"
