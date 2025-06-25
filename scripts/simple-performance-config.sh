#!/bin/bash
# 🚀 Simple Performance Configuration Script
# 
# Usage: ./scripts/simple-performance-config.sh [lite|balanced|performance|auto|status]

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_FILE="$PROJECT_ROOT/.current-config"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to display usage
show_usage() {
    echo "🎯 Medallion Lakehouse Performance Configuration"
    echo "================================================"
    echo ""
    echo "Usage: $0 [lite|balanced|performance|auto|status]"
    echo ""
    echo "Commands:"
    echo "  lite        - Resource-constrained configuration (8GB- RAM)"
    echo "  balanced    - Balanced configuration (default)"
    echo "  performance - High-performance configuration (16GB+ RAM)"
    echo "  auto        - Auto-detect and apply optimal configuration"
    echo "  status      - Show current configuration and system info"
    echo ""
    echo "Examples:"
    echo "  $0 auto        # Auto-detect and apply"
    echo "  $0 performance # Force high-performance"
    echo "  $0 status      # Check current status"
}

# Function to detect system resources
detect_system() {
    echo "🖥️  System Information:"
    
    # Detect OS
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        OS="Linux"
        # Get memory in GB (Linux)
        MEMORY_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
        MEMORY_GB=$((MEMORY_KB / 1024 / 1024))
        # Get CPU count
        CPU_COUNT=$(nproc)
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macOS"
        # Get memory in GB (macOS)
        MEMORY_BYTES=$(sysctl -n hw.memsize)
        MEMORY_GB=$((MEMORY_BYTES / 1024 / 1024 / 1024))
        # Get CPU count
        CPU_COUNT=$(sysctl -n hw.ncpu)
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
        OS="Windows"
        # Windows detection is more complex, use defaults
        MEMORY_GB=8
        CPU_COUNT=4
        echo "   Note: Windows detection limited, using conservative estimates"
    else
        OS="Unknown"
        MEMORY_GB=8
        CPU_COUNT=4
        echo "   Note: OS detection failed, using conservative estimates"
    fi
    
    echo "   OS: $OS"
    echo "   Memory: ${MEMORY_GB}GB"
    echo "   CPU Cores: $CPU_COUNT"
    echo ""
}

# Function to recommend configuration
recommend_config() {
    if [[ $MEMORY_GB -gt 32 ]] && [[ $CPU_COUNT -ge 12 ]]; then
        echo "performance"
    elif [[ $MEMORY_GB -gt 16 ]] && [[ $MEMORY_GB -le 32 ]] && [[ $CPU_COUNT -ge 8 ]]; then
        echo "balanced" 
    else
        echo "lite"
    fi
}

# Function to show recommendations
show_recommendations() {
    echo "🎯 Configuration Recommendations:"
    echo ""
    
    RECOMMENDED=$(recommend_config)
    
    # Lite configuration
    if [[ "$RECOMMENDED" == "lite" ]]; then
        LITE_MARKER="← RECOMMENDED"
    else
        LITE_MARKER=""
    fi
    echo -e "   ${BLUE}LITE${NC}         Standard Developer Configuration $LITE_MARKER"
    echo "               Target: ≤16GB RAM, most developer laptops, CI/testing"
    echo "               Spark: 512m driver, 512m executor"
    echo "               DAG Time: 45-60 seconds"
    echo ""
    
    # Balanced configuration
    if [[ "$RECOMMENDED" == "balanced" ]]; then
        BALANCED_MARKER="← RECOMMENDED"
    else
        BALANCED_MARKER=""
    fi
    echo -e "   ${YELLOW}BALANCED${NC}     Better Developer Machine Configuration $BALANCED_MARKER"
    echo "               Target: 16-32GB RAM, better developer machines"
    echo "               Spark: 1g driver, 2g executor"
    echo "               DAG Time: 30-45 seconds"
    echo ""
    
    # Performance configuration
    if [[ "$RECOMMENDED" == "performance" ]]; then
        PERFORMANCE_MARKER="← RECOMMENDED"
    else
        PERFORMANCE_MARKER=""
    fi
    echo -e "   ${GREEN}PERFORMANCE${NC}  High-End Workstation Configuration $PERFORMANCE_MARKER"
    echo "               Target: >32GB RAM, high-end workstations/desktop rigs"
    echo "               Spark: 2g driver, 4g executor" 
    echo "               DAG Time: 20-30 seconds"
    echo ""
}

# Function to apply configuration
apply_config() {
    local config=$1
    
    echo -e "${GREEN}🚀 Applying $config configuration...${NC}"
    echo ""
    
    # Stop existing containers
    echo "   Stopping existing containers..."
    cd "$PROJECT_ROOT"
    docker-compose down > /dev/null 2>&1 || true
    
    # Build compose command
    case $config in
        "performance")
            COMPOSE_FILES="-f docker-compose.yml -f docker-compose.performance.yml"
            ;;
        "balanced")
            COMPOSE_FILES="-f docker-compose.yml -f docker-compose.balanced.yml"
            ;;
        "lite")
            COMPOSE_FILES="-f docker-compose.yml -f docker-compose.lite.yml"
            ;;
        *)  # default to balanced
            COMPOSE_FILES="-f docker-compose.yml"
            config="balanced"
            ;;
    esac
    
    # Start with selected configuration
    echo "   Starting containers with $config configuration..."
    if docker-compose $COMPOSE_FILES up -d; then
        echo ""
        echo -e "${GREEN}✅ Successfully started with $config configuration!${NC}"
        echo ""
        echo "   Airflow UI: http://localhost:8080"
        echo "   Default credentials: admin/admin"
        
        # Save current configuration
        echo "$config" > "$CONFIG_FILE"
        
        echo ""
        echo "🎉 Configuration applied successfully!"
        echo ""
        echo "Next steps:"
        echo "   1. Wait 30-60 seconds for services to start"
        echo "   2. Open http://localhost:8080 (admin/admin)"
        echo "   3. Trigger bronze_basic_pagila_ingestion DAG to test"
    else
        echo -e "${RED}❌ Failed to start containers${NC}"
        exit 1
    fi
}

# Function to show current status
show_status() {
    echo "📊 Current Status:"
    echo ""
    
    # Show current configuration
    if [[ -f "$CONFIG_FILE" ]]; then
        CURRENT=$(cat "$CONFIG_FILE")
        echo "   Current Configuration: $CURRENT"
    else
        echo "   Current Configuration: unknown"
    fi
    
    # Show running containers
    echo ""
    echo "   Docker Containers:"
    if docker-compose ps --services --filter "status=running" 2>/dev/null | grep -q .; then
        docker-compose ps 2>/dev/null | grep -E "(NAME|airflow|postgres)" || echo "   No medallion containers running"
    else
        echo "   No containers running"
    fi
    
    echo ""
}

# Main script logic
main() {
    # Check if docker-compose is available
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}❌ docker-compose not found. Please install Docker Compose.${NC}"
        exit 1
    fi
    
    # Parse command line argument
    case "${1:-}" in
        "lite"|"balanced"|"performance")
            detect_system
            apply_config "$1"
            ;;
        "auto")
            detect_system
            RECOMMENDED=$(recommend_config)
            echo -e "${BLUE}🤖 Auto-applying recommended configuration: $RECOMMENDED${NC}"
            echo ""
            apply_config "$RECOMMENDED"
            ;;
        "status")
            detect_system
            show_status
            ;;
        "help"|"--help"|"-h")
            show_usage
            ;;
        "")
            detect_system
            show_recommendations
            echo ""
            echo "To apply a configuration:"
            RECOMMENDED=$(recommend_config)
            echo "   $0 auto                    # Apply recommended ($RECOMMENDED)"
            echo "   $0 lite                    # Apply lite"
            echo "   $0 balanced               # Apply balanced"
            echo "   $0 performance            # Apply performance"
            ;;
        *)
            echo -e "${RED}❌ Unknown command: $1${NC}"
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@"