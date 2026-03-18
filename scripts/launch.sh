#!/usr/bin/env bash
# -------------------------------------------------------------------
# launch.sh - Mini Perplexity One-Click Setup & Launch
#
# This script does everything needed to run Mini Perplexity locally:
#   1. Checks prerequisites (Python, uv, Node, Azure CLI)
#   2. Initializes Azure Blob Storage (containers + folder structure)
#   3. Generates .env files from your connection string
#   4. Installs all dependencies (Python + Node)
#   5. Starts all services
#
# Usage:
#   ./scripts/launch.sh
#
# Prerequisites you need before running:
#   - An Azure account with a storage account + service bus namespace
#   - A GPU machine (for embedding + retriever services)
#   - Python 3.10+, uv, Node.js 18+, Azure CLI
# -------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

# -------------------------------------------------------------------
# Step 1: Check prerequisites
# -------------------------------------------------------------------
echo ""
echo "=============================================="
echo "  Mini Perplexity - Launch Script"
echo "=============================================="
echo ""

info "Checking prerequisites..."

command -v python3 >/dev/null 2>&1 || fail "python3 is required. Install Python 3.10+"
command -v uv >/dev/null 2>&1      || fail "uv is required. Install: curl -LsSf https://astral.sh/uv/install.sh | sh"
command -v node >/dev/null 2>&1    || fail "node is required. Install Node.js 18+"
command -v az >/dev/null 2>&1      || warn "Azure CLI not found. You can still run if .env files are already configured."

PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
ok "Python $PYTHON_VERSION"
ok "uv $(uv --version 2>/dev/null | head -1)"
ok "Node $(node --version)"
echo ""

# -------------------------------------------------------------------
# Step 2: Collect Azure credentials (interactive)
# -------------------------------------------------------------------
info "Configuring Azure credentials..."
echo ""

# Check if .env files already exist
EXISTING_ENV=false
if [ -f "$ROOT_DIR/services/retriever/.env" ] && [ -f "$ROOT_DIR/services/insert_index/.env" ]; then
    EXISTING_ENV=true
    warn ".env files already exist."
    read -rp "Use existing .env files? [Y/n]: " USE_EXISTING
    USE_EXISTING="${USE_EXISTING:-Y}"
    if [[ "$USE_EXISTING" =~ ^[Yy] ]]; then
        ok "Using existing .env files"
    else
        EXISTING_ENV=false
    fi
fi

if [ "$EXISTING_ENV" = false ]; then
    echo "Enter your Azure credentials (get these from the Azure Portal):"
    echo ""

    read -rp "Azure Storage Account name: " AZURE_STORAGE_ACCOUNT
    [ -z "$AZURE_STORAGE_ACCOUNT" ] && fail "Storage account name is required"

    read -rp "Azure Storage Connection String: " AZURE_CONN_STR
    [ -z "$AZURE_CONN_STR" ] && fail "Connection string is required"

    read -rp "Azure Service Bus Connection String (leave empty to skip indexer/embedding): " SERVICE_BUS_CONN_STR

    echo ""
    info "Writing .env files..."

    # --- services/retriever/.env ---
    cat > "$ROOT_DIR/services/retriever/.env" <<ENVEOF
AZURE_STORAGE_CONNECTION_STRING=$AZURE_CONN_STR
AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT
AZURE_VECTOR_CONTAINER=vectorindexes
AZURE_BLOB_PREFIX=vector-indexes-client1
ENVEOF
    ok "services/retriever/.env"

    # --- services/insert_index/.env ---
    cat > "$ROOT_DIR/services/insert_index/.env" <<ENVEOF
AZURE_STORAGE_CONNECTION_STRING=$AZURE_CONN_STR
AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT
AZURE_VECTOR_CONTAINER=vectorindexes
SERVICE_BUS_CONN_STR=$SERVICE_BUS_CONN_STR
TOPIC_NAME_INGESTION=ingestion
ENVEOF
    ok "services/insert_index/.env"

    # --- services/indexer/.env ---
    cat > "$ROOT_DIR/services/indexer/.env" <<ENVEOF
AZURE_STORAGE_CONNECTION_STRING=$AZURE_CONN_STR
AZURE_SERVICE_BUS_CONNECTION_STRING=$SERVICE_BUS_CONN_STR
SERVICE_BUS_NAMESPACE=embedding-pipeline-search
SERVICE_BUS_TOPIC=ingestion
CONTAINER_NAME=commoncrawl-wet
MAX_WORDS_PER_CHUNK=300
NUM_WORKERS=4
FILES_PER_WORKER=51
HOST=0.0.0.0
PORT=8001
ENVEOF
    ok "services/indexer/.env"

    # --- services/embedding/.env ---
    cat > "$ROOT_DIR/services/embedding/.env" <<ENVEOF
MODEL_NAME=Qwen/Qwen3-Embedding-4B
MODEL_CACHE_DIR=./models
CUDA_VISIBLE_DEVICES=0
HOST=0.0.0.0
PORT=8000
ENVEOF
    ok "services/embedding/.env"

    # --- data/.env ---
    cat > "$ROOT_DIR/data/.env" <<ENVEOF
AZURE_CONN_STR=$AZURE_CONN_STR
CONTAINER_NAME=fineweb-raw
DATASET_ID=HuggingFaceFW/fineweb
DATASET_SPLIT=train
CHUNK_SIZE=10000
BLOB_PREFIX=fineweb/train
UPLOAD_RETRIES=3
ENVEOF
    ok "data/.env"

    echo ""
fi

# -------------------------------------------------------------------
# Step 3: Initialize Azure Blob Storage
# -------------------------------------------------------------------
if command -v az >/dev/null 2>&1; then
    read -rp "Initialize Azure Blob Storage containers? [Y/n]: " INIT_BLOB
    INIT_BLOB="${INIT_BLOB:-Y}"
    if [[ "$INIT_BLOB" =~ ^[Yy] ]]; then
        # Extract account name from .env if not set
        if [ -z "${AZURE_STORAGE_ACCOUNT:-}" ]; then
            AZURE_STORAGE_ACCOUNT=$(grep AZURE_STORAGE_ACCOUNT "$ROOT_DIR/services/retriever/.env" | cut -d= -f2-)
        fi
        if [ -n "$AZURE_STORAGE_ACCOUNT" ]; then
            bash "$SCRIPT_DIR/init_azure_blob.sh" "$AZURE_STORAGE_ACCOUNT"
        else
            warn "Could not determine storage account name. Run init_azure_blob.sh manually."
        fi
    fi
else
    warn "Azure CLI not installed - skipping blob initialization."
    warn "Run 'scripts/init_azure_blob.sh <account_name>' later to create containers."
fi

echo ""

# -------------------------------------------------------------------
# Step 4: Install dependencies
# -------------------------------------------------------------------
info "Installing dependencies..."
echo ""

# Python services
PYTHON_SERVICES=("services/retriever" "services/insert_index" "services/indexer" "services/embedding" "backend" "data")

for svc in "${PYTHON_SERVICES[@]}"; do
    svc_path="$ROOT_DIR/$svc"
    if [ -f "$svc_path/pyproject.toml" ]; then
        info "Installing $svc ..."
        (cd "$svc_path" && uv sync --quiet 2>&1) && ok "$svc" || warn "Failed to install $svc (may need GPU libs)"
    fi
done

# Frontend
if [ -f "$ROOT_DIR/frontend/package.json" ]; then
    info "Installing frontend..."
    (cd "$ROOT_DIR/frontend" && npm install --silent 2>&1) && ok "frontend" || warn "Failed to install frontend deps"
fi

echo ""

# -------------------------------------------------------------------
# Step 5: Start services
# -------------------------------------------------------------------
info "Starting services..."
echo ""
echo "Services will start in the background. Logs go to /tmp/mini_perplexity_*.log"
echo ""

# Start backend (port 8000)
info "Starting backend (port 8000)..."
(cd "$ROOT_DIR/backend" && uv run python main.py > /tmp/mini_perplexity_backend.log 2>&1) &
BACKEND_PID=$!
ok "Backend started (PID: $BACKEND_PID)"

# Start insert_index service (port 8001)
info "Starting insert_index service (port 8001)..."
(cd "$ROOT_DIR/services/insert_index" && uv run uvicorn server:app --host 0.0.0.0 --port 8001 > /tmp/mini_perplexity_insert_index.log 2>&1) &
INSERT_PID=$!
ok "Insert Index started (PID: $INSERT_PID)"

# Start retriever service (port 8002)
info "Starting retriever service (port 8002)..."
(cd "$ROOT_DIR/services/retriever" && uv run python server.py > /tmp/mini_perplexity_retriever.log 2>&1) &
RETRIEVER_PID=$!
ok "Retriever started (PID: $RETRIEVER_PID)"

# Start frontend (port 5173)
info "Starting frontend (port 5173)..."
(cd "$ROOT_DIR/frontend" && npm run dev > /tmp/mini_perplexity_frontend.log 2>&1) &
FRONTEND_PID=$!
ok "Frontend started (PID: $FRONTEND_PID)"

echo ""
echo "=============================================="
echo "  Mini Perplexity is running!"
echo "=============================================="
echo ""
echo "  Frontend:     http://localhost:5173"
echo "  Backend API:  http://localhost:8000"
echo "  Insert Index: http://localhost:8001"
echo "  Retriever:    http://localhost:8002"
echo ""
echo "  Logs:"
echo "    tail -f /tmp/mini_perplexity_backend.log"
echo "    tail -f /tmp/mini_perplexity_insert_index.log"
echo "    tail -f /tmp/mini_perplexity_retriever.log"
echo "    tail -f /tmp/mini_perplexity_frontend.log"
echo ""
echo "  To stop all services:"
echo "    kill $BACKEND_PID $INSERT_PID $RETRIEVER_PID $FRONTEND_PID"
echo ""

# Wait for all background processes
wait
