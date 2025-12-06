#!/bin/bash

# Install graphviz if needed
# Ubuntu/Debian: sudo apt-get install graphviz
# macOS: brew install graphviz

# Generate PNG
dot -Tpng ../docs/architecture.dot -o ../docs/architecture.png

# Generate SVG (scalable)
dot -Tsvg ../docs/architecture.dot -o ../docs/architecture.svg

# Generate high-res PNG
dot -Tpng -Gdpi=300 ../docs/architecture.dot -o ../docs/architecture_hires.png

echo "✓ Generated architecture diagrams:"
echo "  - architecture.png (standard)"
echo "  - architecture_hires.png (high resolution)"
echo "  - architecture.svg (scalable vector)"
