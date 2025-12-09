#!/bin/bash
#
# Install dependencies for btrdupd
#
# Usage:
#   ./scripts/install_deps.sh          # Install runtime dependencies
#   ./scripts/install_deps.sh --dev    # Include development/testing dependencies
#   ./scripts/install_deps.sh --all    # Install everything
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root"
        exit 1
    fi
}

# Detect package manager
detect_package_manager() {
    if command -v apt &> /dev/null; then
        PKG_MANAGER="apt"
        PKG_INSTALL="apt install -y"
        PKG_UPDATE="apt update"
    elif command -v dnf &> /dev/null; then
        PKG_MANAGER="dnf"
        PKG_INSTALL="dnf install -y"
        PKG_UPDATE="dnf check-update || true"
    elif command -v yum &> /dev/null; then
        PKG_MANAGER="yum"
        PKG_INSTALL="yum install -y"
        PKG_UPDATE="yum check-update || true"
    elif command -v pacman &> /dev/null; then
        PKG_MANAGER="pacman"
        PKG_INSTALL="pacman -S --noconfirm"
        PKG_UPDATE="pacman -Sy"
    else
        log_error "No supported package manager found (apt, dnf, yum, pacman)"
        exit 1
    fi
    log_info "Detected package manager: $PKG_MANAGER"
}

# Install runtime dependencies
install_runtime_deps() {
    log_info "Installing runtime dependencies..."

    case $PKG_MANAGER in
        apt)
            $PKG_INSTALL \
                python3 \
                python3-xxhash \
                python3-tabulate \
                python3-tomlkit \
                duperemove \
                btrfs-progs
            ;;
        dnf|yum)
            $PKG_INSTALL \
                python3 \
                python3-xxhash \
                python3-tabulate \
                python3-tomlkit \
                duperemove \
                btrfs-progs
            ;;
        pacman)
            $PKG_INSTALL \
                python \
                python-xxhash \
                python-tabulate \
                python-tomlkit \
                duperemove \
                btrfs-progs
            ;;
    esac

    log_info "Runtime dependencies installed"
}

# Install development/testing dependencies
install_dev_deps() {
    log_info "Installing development/testing dependencies..."

    case $PKG_MANAGER in
        apt)
            $PKG_INSTALL \
                python3-pytest \
                python3-pip \
                git
            ;;
        dnf|yum)
            $PKG_INSTALL \
                python3-pytest \
                python3-pip \
                git
            ;;
        pacman)
            $PKG_INSTALL \
                python-pytest \
                python-pip \
                git
            ;;
    esac

    log_info "Development dependencies installed"
}

# Verify installations
verify_installations() {
    log_info "Verifying installations..."

    local errors=0

    # Check Python
    if command -v python3 &> /dev/null; then
        log_info "  python3: $(python3 --version)"
    else
        log_error "  python3: NOT FOUND"
        ((errors++))
    fi

    # Check xxhash
    if python3 -c "import xxhash" 2>/dev/null; then
        log_info "  python3-xxhash: OK"
    else
        log_error "  python3-xxhash: NOT FOUND"
        ((errors++))
    fi

    # Check tabulate
    if python3 -c "import tabulate" 2>/dev/null; then
        log_info "  python3-tabulate: OK"
    else
        log_error "  python3-tabulate: NOT FOUND"
        ((errors++))
    fi

    # Check duperemove
    if command -v duperemove &> /dev/null; then
        log_info "  duperemove: $(duperemove --version 2>&1 | head -1)"
    else
        log_error "  duperemove: NOT FOUND"
        ((errors++))
    fi

    # Check btrfs
    if command -v btrfs &> /dev/null; then
        log_info "  btrfs-progs: $(btrfs --version)"
    else
        log_error "  btrfs-progs: NOT FOUND"
        ((errors++))
    fi

    # Check pytest (dev only)
    if [[ "$INSTALL_DEV" == "1" ]]; then
        if python3 -c "import pytest" 2>/dev/null; then
            log_info "  python3-pytest: OK"
        else
            log_warn "  python3-pytest: NOT FOUND (optional for dev)"
        fi
    fi

    if [[ $errors -gt 0 ]]; then
        log_error "Some dependencies are missing!"
        return 1
    fi

    log_info "All dependencies verified successfully"
    return 0
}

# Print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --dev     Install development/testing dependencies"
    echo "  --all     Install all dependencies (runtime + dev)"
    echo "  --verify  Only verify installations, don't install"
    echo "  --help    Show this help message"
    echo ""
    echo "Runtime dependencies:"
    echo "  - python3, python3-xxhash, python3-tabulate"
    echo "  - duperemove, btrfs-progs"
    echo ""
    echo "Development dependencies:"
    echo "  - python3-pytest, python3-pip, git"
}

# Main
main() {
    local INSTALL_RUNTIME=0
    local INSTALL_DEV=0
    local VERIFY_ONLY=0

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --dev)
                INSTALL_DEV=1
                INSTALL_RUNTIME=1
                shift
                ;;
            --all)
                INSTALL_RUNTIME=1
                INSTALL_DEV=1
                shift
                ;;
            --verify)
                VERIFY_ONLY=1
                shift
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    # Default to runtime only if no flags
    if [[ $INSTALL_RUNTIME -eq 0 && $INSTALL_DEV -eq 0 && $VERIFY_ONLY -eq 0 ]]; then
        INSTALL_RUNTIME=1
    fi

    # Export for verify function
    export INSTALL_DEV

    if [[ $VERIFY_ONLY -eq 1 ]]; then
        verify_installations
        exit $?
    fi

    check_root
    detect_package_manager

    log_info "Updating package lists..."
    $PKG_UPDATE

    if [[ $INSTALL_RUNTIME -eq 1 ]]; then
        install_runtime_deps
    fi

    if [[ $INSTALL_DEV -eq 1 ]]; then
        install_dev_deps
    fi

    echo ""
    verify_installations

    echo ""
    log_info "Installation complete!"
}

main "$@"
