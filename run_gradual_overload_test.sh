#!/usr/bin/env bash

# Gradual Overload Test Runner Script
# This script runs the gradual overload test to find the critical failure point

set -euo pipefail

# Get the directory of this script
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
cd "$SCRIPT_DIR" || exit 1

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}===========================================
Starting Gradual Overload Test
===========================================${NC}"

# Check if zkbench binary exists
if [ ! -f "$SCRIPT_DIR/zkbench_gradual" ]; then
    echo -e "${YELLOW}Building zkbench_gradual binary...${NC}"
    go build -o zkbench_gradual main_gradual_overload.go bench_gradual_overload.go
fi

# Default configuration
CONFIG_FILE="${1:-bench_gradual_overload.conf}"
OUTPUT_PREFIX="${2:-zkresult}"

# Parse optional parameters
INITIAL_REQUESTS="${INITIAL_REQUESTS:-50}"
MAX_REQUESTS="${MAX_REQUESTS:-5000}"
STEP_SIZE="${STEP_SIZE:-100}"
STEP_DURATION="${STEP_DURATION:-10}"
WARMUP_STEPS="${WARMUP_STEPS:-5}"
LATENCY_THRESHOLD="${LATENCY_THRESHOLD:-50.0}"
THROUGHPUT_DROP="${THROUGHPUT_DROP:-30.0}"

echo -e "${GREEN}Test Configuration:${NC}"
echo "  Config file: $CONFIG_FILE"
echo "  Output prefix: $OUTPUT_PREFIX"
echo "  Initial requests: $INITIAL_REQUESTS"
echo "  Max requests: $MAX_REQUESTS"
echo "  Step size: $STEP_SIZE"
echo "  Step duration: ${STEP_DURATION}s"
echo "  Warmup steps: $WARMUP_STEPS"
echo "  Latency threshold: ${LATENCY_THRESHOLD}ms"
echo "  Throughput drop threshold: ${THROUGHPUT_DROP}%"
echo ""

# Create output directory
OUTPUT_DIR="${SCRIPT_DIR}/results"
mkdir -p "$OUTPUT_DIR"

# Create metrics directory for agent integration
METRICS_DIR="${SCRIPT_DIR}/../../agent/metrics"
mkdir -p "$METRICS_DIR"

# Clear previous injection markers
rm -f "$METRICS_DIR/main_injection_timestamp.txt"
rm -f "$METRICS_DIR/mitigation_trigger.txt"

# Run the gradual overload test
echo -e "${YELLOW}Running gradual overload test...${NC}"
"$SCRIPT_DIR/zkbench_gradual" \
    -conf "$CONFIG_FILE" \
    -outprefix "$OUTPUT_DIR/$OUTPUT_PREFIX" \
    -initial "$INITIAL_REQUESTS" \
    -max "$MAX_REQUESTS" \
    -step "$STEP_SIZE" \
    -duration "$STEP_DURATION" \
    -warmup "$WARMUP_STEPS" \
    -latency "$LATENCY_THRESHOLD" \
    -throughput "$THROUGHPUT_DROP" \
    -viz

# Check if test completed successfully
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Test completed successfully!${NC}"
    
    # Find the latest results
    LATEST_RESULTS=$(ls -t "$OUTPUT_DIR"/${OUTPUT_PREFIX}-gradual-* 2>/dev/null | head -n1)
    
    if [ -n "$LATEST_RESULTS" ]; then
        echo -e "${BLUE}Results saved to: ${LATEST_RESULTS}*${NC}"
        
        # Check for visualization script
        VIZ_SCRIPT="${LATEST_RESULTS}visualize.py"
        if [ -f "$VIZ_SCRIPT" ]; then
            echo -e "${YELLOW}Generating visualization...${NC}"
            
            # Check if Python and required libraries are installed
            if command -v python3 &> /dev/null; then
                # Try to run visualization
                python3 "$VIZ_SCRIPT" 2>/dev/null || {
                    echo -e "${YELLOW}Note: Visualization requires pandas and matplotlib.${NC}"
                    echo "Install with: pip install pandas matplotlib"
                }
            else
                echo -e "${YELLOW}Python3 not found. Install Python to generate visualizations.${NC}"
            fi
        fi
        
        # Display summary
        SUMMARY_FILE="${LATEST_RESULTS}test_summary.txt"
        if [ -f "$SUMMARY_FILE" ]; then
            echo -e "${GREEN}Test Summary:${NC}"
            cat "$SUMMARY_FILE"
        fi
    fi
else
    echo -e "${RED}Test failed! Check logs for details.${NC}"
    exit 1
fi

echo -e "${BLUE}===========================================${NC}"