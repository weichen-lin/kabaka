#!/bin/bash
set -e

# Configuration
PROJECT_ROOT=$(pwd)
FRONTEND_DIR="$PROJECT_ROOT/dashboard/frontend"
DIST_DIR="$PROJECT_ROOT/dashboard/dist"

echo "🚀 Starting Kabaka Dashboard Release Process..."

# 1. Build Frontend
echo "🏗️  Building Frontend..."
cd "$FRONTEND_DIR"
bun run build

# 2. Sync to Dashboard dist
echo "📦 Syncing static assets to Go embed directory..."
cd "$PROJECT_ROOT"
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"
cp -r "$FRONTEND_DIR/dist/"* "$DIST_DIR/"

# 3. Finalize
echo "✅ Release assets are ready in $DIST_DIR"
echo "👉 You can now commit and tag: git add $DIST_DIR && git commit -m 'Release: Update dashboard assets' && git tag v1.0.x"
