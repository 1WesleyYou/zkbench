# Makefile for ZooKeeper Testing Framework
# Includes standard benchmarks and gradual overload testing

.PHONY: all build test clean help
.PHONY: bench-normal bench-gradual bench-all
.PHONY: deploy start-zk stop-zk
.PHONY: visualize analyze

# Variables
GOPATH ?= $(shell go env GOPATH)
GOCMD = go
GOBUILD = $(GOCMD) build
GOTEST = $(GOCMD) test
GOGET = $(GOCMD) get
GOCLEAN = $(GOCMD) clean

# Python for analysis
PYTHON = python3
PIP = pip3

# Output directories
BUILD_DIR = build
RESULTS_DIR = results
METRICS_DIR = ../../agent/metrics

# Binary names
ZKBENCH_BIN = zkbench
ZKBENCH_GRADUAL_BIN = zkbench_gradual

# Default configuration files
NORMAL_CONF = bench_normal.conf
GRADUAL_CONF = bench_gradual_overload.conf

# Gradual overload test parameters (can be overridden)
INITIAL_REQUESTS ?= 50
MAX_REQUESTS ?= 5000
STEP_SIZE ?= 100
STEP_DURATION ?= 10
WARMUP_STEPS ?= 5
LATENCY_THRESHOLD ?= 50.0
THROUGHPUT_DROP ?= 30.0

# Color output
RED = \033[0;31m
GREEN = \033[0;32m
YELLOW = \033[1;33m
BLUE = \033[0;34m
NC = \033[0m # No Color

## Help
help:
	@echo "$(BLUE)ZooKeeper Testing Framework Makefile$(NC)"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@echo "  $(YELLOW)all$(NC)           - Build all binaries"
	@echo "  $(YELLOW)build$(NC)         - Build zkbench binaries"
	@echo "  $(YELLOW)bench-normal$(NC)  - Run normal benchmark test"
	@echo "  $(YELLOW)bench-gradual$(NC) - Run gradual overload test"
	@echo "  $(YELLOW)bench-all$(NC)     - Run all benchmark tests"
	@echo "  $(YELLOW)visualize$(NC)     - Generate visualizations from latest test"
	@echo "  $(YELLOW)analyze$(NC)       - Analyze test results"
	@echo "  $(YELLOW)clean$(NC)         - Clean build and result files"
	@echo "  $(YELLOW)deploy$(NC)        - Deploy to test environment"
	@echo "  $(YELLOW)start-zk$(NC)      - Start ZooKeeper cluster"
	@echo "  $(YELLOW)stop-zk$(NC)       - Stop ZooKeeper cluster"
	@echo ""
	@echo "$(GREEN)Gradual overload test parameters:$(NC)"
	@echo "  INITIAL_REQUESTS   = $(INITIAL_REQUESTS)"
	@echo "  MAX_REQUESTS       = $(MAX_REQUESTS)"
	@echo "  STEP_SIZE          = $(STEP_SIZE)"
	@echo "  STEP_DURATION      = $(STEP_DURATION)"
	@echo "  WARMUP_STEPS       = $(WARMUP_STEPS)"
	@echo "  LATENCY_THRESHOLD  = $(LATENCY_THRESHOLD)"
	@echo "  THROUGHPUT_DROP    = $(THROUGHPUT_DROP)"
	@echo ""
	@echo "$(GREEN)Examples:$(NC)"
	@echo "  make bench-gradual MAX_REQUESTS=10000"
	@echo "  make bench-gradual LATENCY_THRESHOLD=100"

## Build all binaries
all: build

## Build zkbench binaries
build: $(BUILD_DIR)/$(ZKBENCH_BIN) $(BUILD_DIR)/$(ZKBENCH_GRADUAL_BIN)

$(BUILD_DIR)/$(ZKBENCH_BIN): main.go bench/*.go
	@echo "$(YELLOW)Building $(ZKBENCH_BIN)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(ZKBENCH_BIN) main.go

$(BUILD_DIR)/$(ZKBENCH_GRADUAL_BIN): main_gradual_overload.go bench_gradual_overload.go bench/*.go
	@echo "$(YELLOW)Building $(ZKBENCH_GRADUAL_BIN)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(ZKBENCH_GRADUAL_BIN) main_gradual_overload.go

## Run normal benchmark test
bench-normal: $(BUILD_DIR)/$(ZKBENCH_BIN)
	@echo "$(BLUE)==== Running Normal Benchmark Test ====$(NC)"
	@mkdir -p $(RESULTS_DIR)
	./run_zk_bench.sh

## Run gradual overload test
bench-gradual: $(BUILD_DIR)/$(ZKBENCH_GRADUAL_BIN)
	@echo "$(BLUE)==== Running Gradual Overload Test ====$(NC)"
	@mkdir -p $(RESULTS_DIR)
	@mkdir -p $(METRICS_DIR)
	@rm -f $(METRICS_DIR)/main_injection_timestamp.txt
	@rm -f $(METRICS_DIR)/mitigation_trigger.txt
	INITIAL_REQUESTS=$(INITIAL_REQUESTS) \
	MAX_REQUESTS=$(MAX_REQUESTS) \
	STEP_SIZE=$(STEP_SIZE) \
	STEP_DURATION=$(STEP_DURATION) \
	WARMUP_STEPS=$(WARMUP_STEPS) \
	LATENCY_THRESHOLD=$(LATENCY_THRESHOLD) \
	THROUGHPUT_DROP=$(THROUGHPUT_DROP) \
	./run_gradual_overload_test.sh

## Run all benchmark tests
bench-all: bench-normal bench-gradual
	@echo "$(GREEN)All benchmark tests completed!$(NC)"

## Generate visualizations from latest test results
visualize:
	@echo "$(YELLOW)Generating visualizations...$(NC)"
	@if [ ! -d "$(RESULTS_DIR)" ]; then \
		echo "$(RED)No results directory found. Run tests first.$(NC)"; \
		exit 1; \
	fi
	@LATEST_VIZ=$$(ls -t $(RESULTS_DIR)/*visualize.py 2>/dev/null | head -n1); \
	if [ -z "$$LATEST_VIZ" ]; then \
		echo "$(RED)No visualization script found. Run gradual test first.$(NC)"; \
		exit 1; \
	else \
		echo "$(GREEN)Running visualization script: $$LATEST_VIZ$(NC)"; \
		$(PYTHON) "$$LATEST_VIZ" || { \
			echo "$(YELLOW)Installing required Python packages...$(NC)"; \
			$(PIP) install pandas matplotlib numpy; \
			$(PYTHON) "$$LATEST_VIZ"; \
		}; \
	fi

## Analyze test results
analyze:
	@echo "$(YELLOW)Analyzing test results...$(NC)"
	@if [ ! -d "$(RESULTS_DIR)" ]; then \
		echo "$(RED)No results directory found. Run tests first.$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Latest test results:$(NC)"
	@ls -lht $(RESULTS_DIR) | head -10
	@echo ""
	@LATEST_SUMMARY=$$(ls -t $(RESULTS_DIR)/*test_summary.txt 2>/dev/null | head -n1); \
	if [ -n "$$LATEST_SUMMARY" ]; then \
		echo "$(GREEN)Latest Test Summary:$(NC)"; \
		cat "$$LATEST_SUMMARY"; \
	fi
	@echo ""
	@echo "$(GREEN)Metrics files:$(NC)"
	@ls -lh $(METRICS_DIR)/*.txt 2>/dev/null || echo "No metrics files found"

## Clean build artifacts and results
clean:
	@echo "$(YELLOW)Cleaning build artifacts and results...$(NC)"
	@rm -rf $(BUILD_DIR)
	@rm -f $(ZKBENCH_BIN) $(ZKBENCH_GRADUAL_BIN)
	@echo "$(GREEN)Clean complete!$(NC)"

clean-results:
	@echo "$(YELLOW)Cleaning test results...$(NC)"
	@rm -rf $(RESULTS_DIR)
	@rm -f $(METRICS_DIR)/main_injection_timestamp.txt
	@rm -f $(METRICS_DIR)/mitigation_trigger.txt
	@echo "$(GREEN)Results cleaned!$(NC)"

clean-all: clean clean-results

## Deploy to test environment
deploy:
	@echo "$(YELLOW)Deploying to test environment...$(NC)"
	@# Add your deployment commands here
	@echo "$(GREEN)Deployment complete!$(NC)"

## Start ZooKeeper cluster
start-zk:
	@echo "$(YELLOW)Starting ZooKeeper cluster...$(NC)"
	@# Add commands to start ZooKeeper
	@# Example: ansible-playbook -i inventory start_zookeeper.yml
	@echo "$(GREEN)ZooKeeper cluster started!$(NC)"

## Stop ZooKeeper cluster
stop-zk:
	@echo "$(YELLOW)Stopping ZooKeeper cluster...$(NC)"
	@# Add commands to stop ZooKeeper
	@# Example: ansible-playbook -i inventory stop_zookeeper.yml
	@echo "$(GREEN)ZooKeeper cluster stopped!$(NC)"

## Install dependencies
deps:
	@echo "$(YELLOW)Installing Go dependencies...$(NC)"
	$(GOGET) github.com/samuel/go-zookeeper/zk
	@echo "$(YELLOW)Installing Python dependencies...$(NC)"
	$(PIP) install pandas matplotlib numpy
	@echo "$(GREEN)Dependencies installed!$(NC)"

## Run tests
test:
	@echo "$(YELLOW)Running unit tests...$(NC)"
	$(GOTEST) -v ./...

## Show current configuration
show-config:
	@echo "$(BLUE)Current Configuration:$(NC)"
	@echo "$(GREEN)Normal benchmark config:$(NC)"
	@cat $(NORMAL_CONF) | grep -v "^#" | grep -v "^$$"
	@echo ""
	@echo "$(GREEN)Gradual overload config:$(NC)"
	@cat $(GRADUAL_CONF) | grep -v "^#" | grep -v "^$$"

## Monitor test in real-time (requires gradual test to be running)
monitor:
	@echo "$(YELLOW)Monitoring test metrics...$(NC)"
	@while true; do \
		clear; \
		echo "$(BLUE)==== Real-time Test Monitoring ====$(NC)"; \
		echo ""; \
		if [ -f "$(METRICS_DIR)/main_injection_timestamp.txt" ]; then \
			echo "$(GREEN)Injection started:$(NC)"; \
			tail -n 1 $(METRICS_DIR)/main_injection_timestamp.txt; \
		fi; \
		if [ -f "$(METRICS_DIR)/mitigation_trigger.txt" ]; then \
			echo "$(RED)Mitigation triggered:$(NC)"; \
			tail -n 1 $(METRICS_DIR)/mitigation_trigger.txt; \
		fi; \
		echo ""; \
		LATEST_METRICS=$$(ls -t $(RESULTS_DIR)/*gradual_overload_metrics.csv 2>/dev/null | head -n1); \
		if [ -n "$$LATEST_METRICS" ]; then \
			echo "$(GREEN)Latest metrics:$(NC)"; \
			tail -n 5 "$$LATEST_METRICS" | column -t -s,; \
		fi; \
		sleep 2; \
	done

.PRECIOUS: $(BUILD_DIR)/$(ZKBENCH_BIN) $(BUILD_DIR)/$(ZKBENCH_GRADUAL_BIN)