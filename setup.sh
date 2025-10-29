#!/usr/bin/env bash

# Quick Setup Script for Gradual Overload Test
# This script sets up the environment and runs a sample test

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=========================================="
echo "Gradual Overload Test - Quick Setup"
echo "==========================================${NC}"

# Check for Go installation
echo -e "${YELLOW}Checking prerequisites...${NC}"
if ! command -v go &> /dev/null; then
    echo -e "${RED}Error: Go is not installed${NC}"
    echo "Please install Go from https://golang.org/dl/"
    exit 1
fi
echo -e "${GREEN}✓ Go installed: $(go version)${NC}"

# Check for Python (optional, for visualization)
if command -v python3 &> /dev/null; then
    echo -e "${GREEN}✓ Python3 installed: $(python3 --version)${NC}"
else
    echo -e "${YELLOW}⚠ Python3 not installed (optional, needed for visualization)${NC}"
fi

# Create project structure
echo -e "${YELLOW}Creating project structure...${NC}"
mkdir -p bench
mkdir -p results
mkdir -p ../../agent/metrics

# Check if bench package files exist
if [ ! -f "bench/bench.go" ] || [ ! -f "bench/client.go" ]; then
    echo -e "${YELLOW}Note: bench/*.go files not found${NC}"
    echo "Please ensure you have the complete zkbench source code"
    echo "You can get it from: https://github.com/OrderLab/zkbench"
fi

# Install Go dependencies
echo -e "${YELLOW}Installing Go dependencies...${NC}"
go mod init zkbench-gradual 2>/dev/null || true
go get github.com/samuel/go-zookeeper/zk || {
    echo -e "${YELLOW}Failed to get dependencies. Creating minimal go.mod...${NC}"
    cat > go.mod << 'EOF'
module zkbench-gradual

go 1.19

require github.com/samuel/go-zookeeper/zk v0.0.0-20201211165307-7117e9ea2414
EOF
}

# Build the binaries
echo -e "${YELLOW}Building test binaries...${NC}"
if [ -f "bench/bench.go" ]; then
    echo "Building with existing bench package..."
    go build -o zkbench_gradual main_gradual_overload.go || {
        echo -e "${RED}Build failed. Please check that all bench/*.go files are present${NC}"
        exit 1
    }
else
    echo -e "${YELLOW}Creating standalone version (limited functionality)...${NC}"
    # Create a minimal standalone version for testing
    cat > zkbench_gradual_standalone.go << 'EOF'
package main

import (
    "fmt"
    "log"
    "time"
)

func main() {
    log.Println("Gradual Overload Test - Standalone Demo")
    log.Println("This is a demonstration of the test phases")
    
    phases := []string{"INIT", "WARMUP", "LOAD_INCREASE", "FAILURE", "MITIGATION", "RECOVERED"}
    workloads := []int{50, 100, 500, 2300, 1610, 1610}
    
    for i, phase := range phases {
        log.Printf("Phase: %s - Workload: %d requests", phase, workloads[i])
        time.Sleep(2 * time.Second)
    }
    
    log.Println("Test complete!")
    log.Println("In a real test, this would interact with ZooKeeper and collect metrics")
}
EOF
    go build -o zkbench_gradual_demo zkbench_gradual_standalone.go
    echo -e "${GREEN}✓ Demo binary created${NC}"
fi

# Make scripts executable
echo -e "${YELLOW}Setting up execution scripts...${NC}"
chmod +x run_gradual_overload_test.sh 2>/dev/null || true

# Install Python dependencies (optional)
if command -v pip3 &> /dev/null; then
    echo -e "${YELLOW}Installing Python visualization dependencies...${NC}"
    pip3 install --quiet pandas matplotlib numpy 2>/dev/null || {
        echo -e "${YELLOW}Failed to install Python packages. Visualization may not work.${NC}"
    }
fi

echo -e "${GREEN}=========================================="
echo "Setup Complete!"
echo "==========================================${NC}"
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo "1. Configure your ZooKeeper connection in bench_gradual_overload.conf"
echo "   Current setting: server.0=10.10.1.4:2181"
echo ""
echo "2. Run the test with default settings:"
echo "   ${GREEN}make bench-gradual${NC}"
echo ""
echo "3. Or run with custom parameters:"
echo "   ${GREEN}make bench-gradual MAX_REQUESTS=10000 LATENCY_THRESHOLD=100${NC}"
echo ""
echo "4. Monitor the test in real-time (in another terminal):"
echo "   ${GREEN}make monitor${NC}"
echo ""
echo "5. After test completion, visualize results:"
echo "   ${GREEN}make visualize${NC}"
echo ""
echo -e "${YELLOW}Documentation:${NC}"
echo "- README.md - Complete documentation"
echo "- EXAMPLE_OUTPUT.md - Sample test outputs"
echo ""

# Offer to run a demo
echo -e "${BLUE}Would you like to run a quick demo? (y/n)${NC}"
read -r response
if [[ "$response" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Running demo...${NC}"
    if [ -f "zkbench_gradual_demo" ]; then
        ./zkbench_gradual_demo
    else
        echo "Demo mode: Simulating test phases..."
        for phase in INIT WARMUP LOAD_INCREASE FAILURE MITIGATION RECOVERED; do
            echo "Phase: $phase"
            sleep 1
        done
    fi
    echo -e "${GREEN}Demo complete!${NC}"
fi

echo -e "${GREEN}Setup finished. Happy testing!${NC}"