#!/usr/bin/env bash
# -------------------------------------------------------------------
# init_azure_blob.sh
#
# Creates the Azure Blob Storage containers and folder structure
# required by Mini Perplexity.
#
# Prerequisites:
#   - Azure CLI installed and logged in (`az login`)
#   - A storage account already created
#
# Usage:
#   ./scripts/init_azure_blob.sh <storage_account_name>
#
# This will create:
#   Container: vectorindexes
#     └── vector-indexes-client1/
#         ├── centroids.npy          (placeholder)
#         ├── metadata.json          (empty root metadata)
#         └── shards/
#             └── init.txt           (placeholder to create folder)
#
#   Container: fineweb-raw           (for FineWeb dataset ingestion)
#
#   Container: commoncrawl-wet       (for CommonCrawl data, optional)
# -------------------------------------------------------------------
set -euo pipefail

STORAGE_ACCOUNT="${1:-}"

if [ -z "$STORAGE_ACCOUNT" ]; then
    echo "Usage: $0 <storage_account_name>"
    echo ""
    echo "Example: $0 mystorageaccount"
    exit 1
fi

echo "=== Mini Perplexity - Azure Blob Storage Initialization ==="
echo "Storage account: $STORAGE_ACCOUNT"
echo ""

# ---------- Create containers ----------
CONTAINERS=("vectorindexes" "fineweb-raw" "commoncrawl-wet")

for container in "${CONTAINERS[@]}"; do
    echo "Creating container: $container"
    az storage container create \
        --account-name "$STORAGE_ACCOUNT" \
        --name "$container" \
        --auth-mode login \
        2>/dev/null && echo "  -> Created '$container'" \
        || echo "  -> '$container' already exists (OK)"
done

echo ""

# ---------- Create vector index folder structure ----------
PREFIX="vector-indexes-client1"
CONTAINER="vectorindexes"

echo "Setting up vector index structure in $CONTAINER/$PREFIX ..."

# Upload placeholder centroids.npy (empty numpy header - services will overwrite on first run)
echo -n "" | az storage blob upload \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$CONTAINER" \
    --name "$PREFIX/centroids.npy" \
    --data "" \
    --overwrite \
    --auth-mode login \
    --only-show-errors \
    && echo "  -> $PREFIX/centroids.npy (placeholder)"

# Upload empty root metadata.json
echo '{}' | az storage blob upload \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$CONTAINER" \
    --name "$PREFIX/metadata.json" \
    --data '{}' \
    --overwrite \
    --auth-mode login \
    --only-show-errors \
    && echo "  -> $PREFIX/metadata.json"

# Upload shards/ folder placeholder
echo -n "" | az storage blob upload \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$CONTAINER" \
    --name "$PREFIX/shards/init.txt" \
    --data "" \
    --overwrite \
    --auth-mode login \
    --only-show-errors \
    && echo "  -> $PREFIX/shards/init.txt (folder placeholder)"

echo ""
echo "=== Azure Blob Storage initialization complete ==="
echo ""
echo "Your storage structure:"
echo "  $CONTAINER/"
echo "    └── $PREFIX/"
echo "        ├── centroids.npy"
echo "        ├── metadata.json"
echo "        └── shards/"
echo "  fineweb-raw/       (ready for FineWeb ingestion)"
echo "  commoncrawl-wet/   (ready for CommonCrawl data)"
echo ""
echo "Next: Get your connection string with:"
echo "  az storage account show-connection-string --name $STORAGE_ACCOUNT --query connectionString -o tsv"
