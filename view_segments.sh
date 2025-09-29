#!/bin/bash
# View all segments in MinIO

echo "📦 Segments in MinIO:"
mc ls scribe/hyra-scribe-ledger/segments/ || echo "No segments yet"
echo ""
echo "📄 Manifest:"
mc cat scribe/hyra-scribe-ledger/manifest.json 2>/dev/null | jq || echo "No manifest yet"
