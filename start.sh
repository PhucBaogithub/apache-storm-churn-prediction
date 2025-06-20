#!/bin/bash

echo "🚀 Starting Storm Churn Prediction System"
echo "=========================================="

# Start Storm services
echo "1. Starting Storm services..."
storm nimbus &
sleep 3
storm supervisor &
sleep 3

# Generate sample data
echo "2. Generating sample data..."
python create_real_storm_outputs.py

# Start Flask application
echo "3. Starting Flask web application..."
cd webapp
python app.py

echo "✅ System ready at http://localhost:5001" 