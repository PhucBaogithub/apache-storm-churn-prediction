#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${PURPLE}🎯 COMPLETE APACHE STORM + FLASK SYSTEM STARTUP${NC}"
echo -e "${PURPLE}=================================================${NC}"

# 1. Check if Storm cluster is running
echo -e "${YELLOW}1. Checking Storm cluster status...${NC}"
if pgrep -f "storm nimbus" > /dev/null; then
    echo -e "${GREEN}   ✅ Storm Nimbus is running${NC}"
else
    echo -e "${YELLOW}   🔄 Starting Storm Nimbus...${NC}"
    storm nimbus &
    sleep 5
fi

if pgrep -f "storm supervisor" > /dev/null; then
    echo -e "${GREEN}   ✅ Storm Supervisor is running${NC}"
else
    echo -e "${YELLOW}   🔄 Starting Storm Supervisor...${NC}"
    storm supervisor &
    sleep 3
fi

# 2. Generate initial data
echo -e "${YELLOW}2. Generating real-time Storm outputs...${NC}"
python create_real_storm_outputs.py

# 3. Stop old Streamlit
echo -e "${YELLOW}3. Stopping old Streamlit interface...${NC}"
pkill -f streamlit 2>/dev/null || true

# 4. Start Flask Web Application
echo -e "${YELLOW}4. Starting Modern Flask Web Application...${NC}"
cd webapp
python app.py &
FLASK_PID=$!
cd ..

sleep 5

# 5. Start real-time data generation
echo -e "${YELLOW}5. Starting continuous data generation...${NC}"
python deploy_storm_with_outputs.py &
DATA_PID=$!

echo ""
echo -e "${GREEN}🎉 SYSTEM STARTUP COMPLETE!${NC}"
echo -e "${GREEN}============================${NC}"
echo ""
echo -e "${BLUE}📊 Access Points:${NC}"
echo -e "${BLUE}  • Modern Web UI: http://localhost:5001${NC}"
echo -e "${BLUE}  • Storm UI (optional): http://localhost:8080${NC}"
echo ""
echo -e "${PURPLE}🚀 Features Available:${NC}"
echo -e "${PURPLE}  • Real-time churn prediction${NC}"
echo -e "${PURPLE}  • Interactive CSV log output${NC}"
echo -e "${PURPLE}  • Auto-refreshing dashboard${NC}"
echo -e "${PURPLE}  • Storm topology processing${NC}"
echo -e "${PURPLE}  • ML model predictions${NC}"
echo -e "${PURPLE}  • Data download functionality${NC}"
echo ""
echo -e "${YELLOW}📱 The new interface includes:${NC}"
echo -e "${YELLOW}  • Beautiful modern UI design${NC}"
echo -e "${YELLOW}  • Real-time data refresh every 5s${NC}"
echo -e "${YELLOW}  • Mobile responsive layout${NC}"
echo -e "${YELLOW}  • Interactive prediction form${NC}"
echo -e "${YELLOW}  • Enhanced log output with tabs${NC}"
echo -e "${YELLOW}  • Statistics dashboard${NC}"
echo ""
echo -e "${RED}Press Ctrl+C to stop all services${NC}"

# Wait for user interrupt
trap 'echo -e "\n${YELLOW}Stopping all services...${NC}"; kill $FLASK_PID $DATA_PID 2>/dev/null; exit 0' INT

# Keep script running
while true; do
    sleep 10
    echo -e "${GREEN}🔄 System running... (Flask: $FLASK_PID, Data: $DATA_PID)${NC}"
done 