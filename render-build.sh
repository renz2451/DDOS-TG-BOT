#!/bin/bash

echo "🚀 Starting Render build..."

# Install Python dependencies
pip install -r requirements.txt

# Make script executable
chmod +x PrimeLeaks.py

echo "✅ Build complete!"