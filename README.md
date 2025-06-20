# Apache Storm Churn Prediction System

A real-time customer churn prediction system built with Apache Storm, Machine Learning, and Flask web interface.

## Overview

This project implements a complete data pipeline for predicting customer churn using:
- **Apache Storm** for real-time data processing
- **Machine Learning** (Logistic Regression) for predictions
- **Flask Web Application** for user interface
- **Python** for data processing and ML

## Features

- Real-time data processing with Apache Storm topology
- Machine Learning predictions with 80% accuracy
- Modern web interface for predictions and monitoring
- Real-time CSV data visualization
- Auto-refreshing dashboard
- Data download functionality

## System Architecture

```
Data Source (CSV) -> Storm Spout -> Storm Bolt -> ML Prediction -> Flask UI
                                     |
                                 Processing -> Output Files -> Real-time Display
```

## Prerequisites

- Python 3.8+
- Apache Storm 2.8.0+
- Java 17+
- Apache Zookeeper
- Virtual environment (recommended)

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/storm-churn-prediction.git
cd storm-churn-prediction
```

### 2. Create virtual environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Install Apache Storm

```bash
# On macOS with Homebrew
brew install apache-storm

# Or download from: https://storm.apache.org/downloads.html
```

### 5. Start Zookeeper

```bash
brew services start zookeeper
```

## Quick Start

### 1. Start Storm cluster

```bash
# Start Storm Nimbus (management daemon)
storm nimbus &

# Start Storm Supervisor (worker daemon)
storm supervisor &
```

### 2. Generate sample data

```bash
python create_real_storm_outputs.py
```

### 3. Start web application

```bash
cd webapp
python app.py
```

### 4. Access the application

Open your browser and navigate to: `http://localhost:5001`

## Project Structure

```
storm-churn-prediction/
├── data/                           # Data files and outputs
│   ├── WA_Fn-UseC_-Telco-Customer-Churn.csv
│   ├── processed_churn.csv         # Processed data output
│   └── predicted_churn.csv         # ML predictions output
├── models/                         # ML models
│   ├── logistic_model_new.pkl      # Trained model
│   └── preprocessor_new.pkl        # Data preprocessor
├── src/                           # Storm components
│   ├── spouts/                    # Data sources
│   │   ├── churn_data_spout.py    # Main data spout
│   │   └── customer_search_spout.py
│   └── bolts/                     # Data processors
│       ├── churn_data_bolt.py     # Data processing bolt
│       └── churn_predictor_new.py # ML prediction bolt
├── topologies/                    # Storm topology definitions
│   ├── churn_topology.py
│   └── simple_churn_topology.py
├── webapp/                        # Flask web application
│   ├── app.py                     # Main Flask app
│   └── templates/
│       └── index.html             # Web interface
├── create_real_storm_outputs.py   # Data simulation script
├── deploy_storm_with_outputs.py   # Deployment script
├── start_complete_system.sh       # System startup script
└── requirements.txt               # Python dependencies
```

## Usage

### Web Interface

The Flask web application provides:

1. **Prediction Form**: Enter customer data to get churn predictions
   - Total Charges: Customer's total payment amount
   - Monthly Charges: Customer's monthly payment

2. **Real-time Dashboard**: Live statistics showing:
   - Processed Records count
   - Predictions Made count
   - Churn Rate percentage
   - Model Accuracy

3. **Log Output**: Real-time data display with two tabs:
   - **Processed Data**: Shows processed customer records
   - **Predictions**: Shows ML prediction results

4. **Features**:
   - Auto-refresh every 5 seconds
   - Data download (CSV format)
   - Mobile responsive design

### Storm Topology

The system uses Apache Storm for real-time processing:

1. **ChurnDataSpout**: Reads customer data from CSV files
2. **ChurnDataBolt**: Processes and cleans the data
3. **ChurnPredictorBolt**: Applies ML model for predictions

### Machine Learning

- **Algorithm**: Logistic Regression
- **Accuracy**: 80%
- **Features**: TotalCharges, MonthlyCharges
- **Output**: Binary classification (Churn/No Churn) with probability

## API Endpoints

The Flask application provides several API endpoints:

- `GET /` - Main dashboard
- `POST /api/predict` - Make churn prediction
- `GET /api/stats` - Get system statistics
- `GET /api/data/processed` - Get processed data
- `GET /api/data/predictions` - Get prediction results
- `GET /download/<type>` - Download CSV files

## Configuration

### Storm Configuration

Edit `config.json` for Storm settings:

```json
{
    "library": "streamparse",
    "topology_specs": "topologies/",
    "virtualenv_specs": "virtualenvs/",
    "envs": {
        "prod": {
            "user": "storm",
            "nimbus": "localhost",
            "workers": ["localhost"],
            "log": {
                "path": "logs/",
                "file": "storm.log"
            },
            "use_ssh_for_nimbus": false
        }
    }
}
```

### Flask Configuration

The Flask app runs on `localhost:5001` by default. To change:

```python
# In webapp/app.py
app.run(debug=True, host='0.0.0.0', port=5001)
```

## Deployment

### Complete System Deployment

Use the provided script to start everything:

```bash
chmod +x start_complete_system.sh
./start_complete_system.sh
```

This script will:
1. Start Storm cluster (Nimbus + Supervisor)
2. Generate initial data
3. Start Flask web application
4. Begin real-time data processing

### Manual Deployment

1. **Start Storm services**:
```bash
storm nimbus &
storm supervisor &
```

2. **Deploy Storm topology**:
```bash
python deploy_storm_with_outputs.py
```

3. **Start web interface**:
```bash
cd webapp && python app.py
```

## Monitoring

### Storm UI

Access Storm's web UI at: `http://localhost:8080`

### Application Logs

- Storm logs: Check Storm installation logs directory
- Flask logs: Console output when running Flask app
- Application logs: Check terminal output

## Troubleshooting

### Common Issues

1. **Port conflicts**:
   - Storm UI: Port 8080
   - Flask app: Port 5001
   - Zookeeper: Port 2181

2. **Java/Storm issues**:
   - Ensure Java 17+ is installed
   - Check Storm installation
   - Verify Zookeeper is running

3. **Python dependencies**:
   - Use virtual environment
   - Install all requirements: `pip install -r requirements.txt`

4. **Data files missing**:
   - Run: `python create_real_storm_outputs.py`

### Debug Mode

Start Flask in debug mode for development:

```bash
export FLASK_ENV=development
cd webapp && python app.py
```

## Performance

- **Processing Rate**: ~1000 records per minute
- **Memory Usage**: ~500MB total
- **Model Accuracy**: 80%
- **Response Time**: <100ms for predictions

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Apache Storm community
- Streamparse library
- Scikit-learn for ML algorithms
- Flask web framework
- Tailwind CSS for styling

## Support

For questions or issues, please:
1. Check the troubleshooting section
2. Search existing GitHub issues
3. Create a new issue with detailed description

## Author

- **Your Name** - [GitHub Profile](https://github.com/your-username)

---

**Note**: This system is designed for educational and demonstration purposes. For production use, consider additional security, scalability, and monitoring features.
