#!/usr/bin/env bash
# Load testing script for peek
#
# Prerequisites:
#   Install hey before running this script.
#
# Usage:
#   ./scripts/load-test.sh [server_url] [subdomain] [domain]
#
# Examples:
#   ./scripts/load-test.sh http://localhost:8080 mysubdomain example.com

set -euo pipefail

SERVER_URL="${1:-http://localhost:8080}"
SUBDOMAIN="${2:-testsubdomain}"
DOMAIN="${3:-localhost}"

HOST="${SUBDOMAIN}.${DOMAIN}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  peek Load Test${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Target: ${GREEN}${SERVER_URL}${NC}"
echo -e "Host:   ${GREEN}${HOST}${NC}"
echo ""

# Check for hey
if ! command -v hey &> /dev/null; then
    echo -e "${RED}Error: 'hey' is not installed.${NC}"
    echo "Install hey, then run this script again."
    exit 1
fi

# Check if the server is reachable
echo -e "${YELLOW}Checking server connectivity...${NC}"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "Host: ${HOST}" "${SERVER_URL}/" 2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "000" ]; then
    echo -e "${RED}Error: Cannot reach server at ${SERVER_URL}${NC}"
    echo "Make sure the server is running and a tunnel is connected."
    exit 1
fi
echo -e "${GREEN}Server reachable (HTTP ${HTTP_CODE})${NC}"
echo ""

# --- Test 1: Throughput (GET requests) ---
echo -e "${YELLOW}Test 1: GET Throughput${NC}"
echo -e "  200 requests, 10 concurrent workers"
echo ""
hey -n 200 -c 10 -host "${HOST}" "${SERVER_URL}/"
echo ""

# --- Test 2: Sustained load ---
echo -e "${YELLOW}Test 2: Sustained Load (30s)${NC}"
echo -e "  30 seconds, 20 concurrent workers"
echo ""
hey -z 30s -c 20 -host "${HOST}" "${SERVER_URL}/"
echo ""

# --- Test 3: POST with body ---
echo -e "${YELLOW}Test 3: POST with 1KB Body${NC}"
echo -e "  100 requests, 10 concurrent workers"
echo ""
BODY=$(python3 -c "print('x' * 1024)" 2>/dev/null || printf 'x%.0s' {1..1024})
hey -n 100 -c 10 -m POST -d "${BODY}" -host "${HOST}" "${SERVER_URL}/echo"
echo ""

# --- Test 4: High concurrency spike ---
echo -e "${YELLOW}Test 4: Concurrency Spike${NC}"
echo -e "  500 requests, 50 concurrent workers"
echo ""
hey -n 500 -c 50 -host "${HOST}" "${SERVER_URL}/"
echo ""

# --- Test 5: Large body ---
echo -e "${YELLOW}Test 5: Large Body (100KB POST)${NC}"
echo -e "  50 requests, 5 concurrent workers"
echo ""
LARGE_BODY=$(python3 -c "print('x' * 102400)" 2>/dev/null || dd if=/dev/zero bs=102400 count=1 2>/dev/null | tr '\0' 'x')
hey -n 50 -c 5 -m POST -d "${LARGE_BODY}" -host "${HOST}" "${SERVER_URL}/echo"
echo ""

echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}  Load test complete!${NC}"
echo -e "${BLUE}========================================${NC}"
