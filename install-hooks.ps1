# PowerShell script to install git hooks
# Run this script to install the pre-commit hook

# Get the git hooks directory
$gitDir = git rev-parse --git-dir
$hooksDir = Join-Path $gitDir "hooks"

# Ensure the hooks directory exists
if (-not (Test-Path $hooksDir)) {
    New-Item -ItemType Directory -Path $hooksDir | Out-Null
    Write-Host "Created hooks directory: $hooksDir"
}

# Copy the pre-commit hook
$source = Join-Path $PWD "pre-commit"
$destination = Join-Path $hooksDir "pre-commit"

Copy-Item -Path $source -Destination $destination -Force
Write-Host "Installed pre-commit hook to $destination"

# Make the hook executable (Windows doesn't need this, but included for completeness)
icacls $destination /grant Everyone:RX | Out-Null
Write-Host "Made pre-commit hook executable"

Write-Host "Git hooks installation complete!"
