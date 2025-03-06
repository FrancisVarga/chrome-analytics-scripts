#!/bin/bash
# Bash script to install git hooks
# Run this script to install the pre-commit hook

# Exit on error
set -e

echo "Installing git hooks..."

# Get the git hooks directory
GIT_DIR=$(git rev-parse --git-dir)
HOOKS_DIR="$GIT_DIR/hooks"

# Ensure the hooks directory exists
if [ ! -d "$HOOKS_DIR" ]; then
    mkdir -p "$HOOKS_DIR"
    echo "Created hooks directory: $HOOKS_DIR"
fi

# Copy the pre-commit hook
SOURCE="$(pwd)/pre-commit"
DESTINATION="$HOOKS_DIR/pre-commit"

cp "$SOURCE" "$DESTINATION"
echo "Installed pre-commit hook to $DESTINATION"

# Make the hook executable
chmod +x "$DESTINATION"
echo "Made pre-commit hook executable"

echo "Git hooks installation complete!"
