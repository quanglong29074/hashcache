#!/bin/bash
# setup_minio.sh - Script để setup và test với MinIO

set -e  # Exit on error

echo "🚀 Hyra Scribe Ledger - MinIO Setup"
echo "===================================="

# MinIO Configuration
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
BUCKET_NAME="${BUCKET_NAME:-hyra-scribe-ledger}"
FLUSH_THRESHOLD="${FLUSH_THRESHOLD:-100}"

echo ""
echo "📋 Configuration:"
echo "  MinIO Endpoint: $MINIO_ENDPOINT"
echo "  Access Key: $MINIO_ACCESS_KEY"
echo "  Bucket: $BUCKET_NAME"
echo "  Flush Threshold: $FLUSH_THRESHOLD"
echo ""

# Check if MinIO is running
echo "🔍 Checking MinIO connection..."
if ! curl -s "$MINIO_ENDPOINT/minio/health/live" > /dev/null 2>&1; then
    echo "❌ MinIO is not running at $MINIO_ENDPOINT"
    echo ""
    echo "💡 Start MinIO with:"
    echo "   docker run -d -p 9000:9000 -p 9001:9001 \\"
    echo "     --name minio \\"
    echo "     -e MINIO_ROOT_USER=$MINIO_ACCESS_KEY \\"
    echo "     -e MINIO_ROOT_PASSWORD=$MINIO_SECRET_KEY \\"
    echo "     minio/minio server /data --console-address ':9001'"
    echo ""
    echo "   Or if using existing MinIO, update variables above"
    exit 1
fi
echo "✅ MinIO is running"

# Install mc (MinIO Client) if not exists
if ! command -v mc &> /dev/null; then
    echo "📦 Installing MinIO Client (mc)..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install minio/stable/mc
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        wget https://dl.min.io/client/mc/release/linux-amd64/mc
        chmod +x mc
        sudo mv mc /usr/local/bin/
    else
        echo "⚠️  Please install mc manually: https://min.io/docs/minio/linux/reference/minio-mc.html"
        exit 1
    fi
fi

# Configure mc alias
echo "🔧 Configuring MinIO client..."
mc alias set scribe "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" > /dev/null 2>&1

# Create bucket if not exists
echo "🪣 Creating bucket: $BUCKET_NAME"
if mc ls scribe/$BUCKET_NAME > /dev/null 2>&1; then
    echo "✅ Bucket already exists"
else
    mc mb scribe/$BUCKET_NAME
    echo "✅ Bucket created"
fi

# Set bucket policy to public read (optional, for testing)
cat > /tmp/bucket-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::$BUCKET_NAME/*"]
    }
  ]
}
EOF

mc anonymous set-json /tmp/bucket-policy.json scribe/$BUCKET_NAME > /dev/null 2>&1
rm /tmp/bucket-policy.json
echo "✅ Bucket policy configured"

# Create .env file for Rust application
echo "📝 Creating .env file..."
cat > .env << EOF
# MinIO Configuration
AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
AWS_ENDPOINT_URL=$MINIO_ENDPOINT
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1

# Application Configuration
S3_BUCKET=$BUCKET_NAME
FLUSH_THRESHOLD=$FLUSH_THRESHOLD

# SDK Configuration
AWS_EC2_METADATA_DISABLED=true
AWS_SDK_LOAD_CONFIG=1
EOF
echo "✅ .env file created"

# Create test data script
echo "📝 Creating test scripts..."
cat > test_insert.sh << 'EOF'
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
EOF
chmod +x test_insert.sh

cat > test_flush.sh << 'EOF'
#!/bin/bash
# Trigger manual flush

echo "Triggering manual flush..."
response=$(curl -s -X POST http://localhost:3000/flush)
echo "$response"
echo ""
echo "Check manifest: curl http://localhost:3000/manifest | jq"
EOF
chmod +x test_flush.sh

cat > test_read.sh << 'EOF'
#!/bin/bash
# Test reading keys

KEY=${1:-test_key_1}
echo "Reading key: $KEY"
curl http://localhost:3000/$KEY
echo ""
EOF
chmod +x test_read.sh

cat > view_segments.sh << 'EOF'
#!/bin/bash
# View all segments in MinIO

echo "📦 Segments in MinIO:"
mc ls scribe/hyra-scribe-ledger/segments/ || echo "No segments yet"
echo ""
echo "📄 Manifest:"
mc cat scribe/hyra-scribe-ledger/manifest.json 2>/dev/null | jq || echo "No manifest yet"
EOF
chmod +x view_segments.sh

echo "✅ Test scripts created:"
echo "   ./test_insert.sh [count]  - Insert test data"
echo "   ./test_flush.sh           - Trigger manual flush"
echo "   ./test_read.sh [key]      - Read a key"
echo "   ./view_segments.sh        - View S3 segments"

echo ""
echo "✨ Setup complete!"
echo ""
echo "Next steps:"
echo "  1. cargo build --release"
echo "  2. source .env"
echo "  3. cargo run"
echo "  4. In another terminal: ./test_insert.sh 150"
echo ""
echo "🌐 MinIO Console: http://localhost:9001"
echo "   Username: $MINIO_ACCESS_KEY"
echo "   Password: $MINIO_SECRET_KEY"