#!/usr/bin/env python
"""
Modern Flask Web Application for Churn Prediction
Thay thế Streamlit với giao diện đẹp mắt hơn
"""
import os
import sys
from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import numpy as np
import joblib
import json
from datetime import datetime
import logging

# Add parent directory to path để import models
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__)
app.secret_key = 'churn_prediction_secret_key'

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths to models and data
MODEL_PATH = "../models/logistic_model_new.pkl"
PREPROCESSOR_PATH = "../models/preprocessor_new.pkl"
DATA_DIR = "../data"

class ChurnPredictor:
    def __init__(self):
        self.model = None
        self.preprocessor = None
        self.load_models()
    
    def load_models(self):
        """Load ML models"""
        try:
            self.model = joblib.load(MODEL_PATH)
            self.preprocessor = joblib.load(PREPROCESSOR_PATH)
            logger.info("✅ ML models loaded successfully")
        except Exception as e:
            logger.error(f"❌ Error loading models: {e}")
            self.model = None
            self.preprocessor = None
    
    def predict(self, total_charges, monthly_charges):
        """Make churn prediction"""
        if not self.model or not self.preprocessor:
            return None, None, "Models not loaded"
        
        try:
            # Create input DataFrame
            input_data = pd.DataFrame([{
                'TotalCharges': float(total_charges),
                'MonthlyCharges': float(monthly_charges)
            }])
            
            # Transform data
            X_processed = self.preprocessor.transform(input_data)
            
            # Make prediction
            prediction = int(self.model.predict(X_processed)[0])
            probability = float(self.model.predict_proba(X_processed)[0][1])
            
            return prediction, probability, "success"
            
        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return None, None, f"Error: {str(e)}"

# Initialize predictor
predictor = ChurnPredictor()

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/predict', methods=['POST'])
def predict_churn():
    """API endpoint for churn prediction"""
    try:
        data = request.get_json()
        total_charges = data.get('total_charges', 0)
        monthly_charges = data.get('monthly_charges', 0)
        
        # Make prediction
        prediction, probability, status = predictor.predict(total_charges, monthly_charges)
        
        if status != "success":
            return jsonify({
                'success': False,
                'error': status
            }), 400
        
        # Determine churn status
        churn_status = "Khách hàng có khả năng rời đi!" if prediction == 1 else "Khách hàng không rời đi."
        confidence = "Cao" if probability > 0.7 or probability < 0.3 else "Trung bình"
        
        return jsonify({
            'success': True,
            'prediction': prediction,
            'probability': round(probability * 100, 2),
            'churn_status': churn_status,
            'confidence': confidence,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
    except Exception as e:
        logger.error(f"API prediction error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/data/processed')
def get_processed_data():
    """Get processed data for log output"""
    try:
        file_path = os.path.join(DATA_DIR, 'processed_churn.csv')
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            
            # Get latest 50 records
            latest_data = df.tail(50).to_dict('records')
            
            return jsonify({
                'success': True,
                'data': latest_data,
                'total_records': len(df),
                'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Processed data file not found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error reading processed data: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/data/predictions')
def get_predictions_data():
    """Get predictions data for log output"""
    try:
        file_path = os.path.join(DATA_DIR, 'predicted_churn.csv')
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            
            # Get latest 50 records
            latest_data = df.tail(50).to_dict('records')
            
            return jsonify({
                'success': True,
                'data': latest_data,
                'total_records': len(df),
                'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Predictions data file not found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error reading predictions data: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/stats')
def get_statistics():
    """Get system statistics"""
    try:
        stats = {
            'total_processed': 0,
            'total_predictions': 0,
            'churn_rate': 0,
            'accuracy': 80.0,  # From our model
            'last_update': 'N/A'
        }
        
        # Count processed records
        processed_file = os.path.join(DATA_DIR, 'processed_churn.csv')
        if os.path.exists(processed_file):
            df_processed = pd.read_csv(processed_file)
            stats['total_processed'] = len(df_processed)
        
        # Count predictions
        predictions_file = os.path.join(DATA_DIR, 'predicted_churn.csv')
        if os.path.exists(predictions_file):
            df_pred = pd.read_csv(predictions_file)
            stats['total_predictions'] = len(df_pred)
            
            # Calculate churn rate if data exists
            if len(df_pred) > 0:
                churn_count = df_pred['Predicted_Churn'].sum() if 'Predicted_Churn' in df_pred.columns else 0
                stats['churn_rate'] = round((churn_count / len(df_pred)) * 100, 2)
                stats['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/download/<data_type>')
def download_data(data_type):
    """Download CSV files"""
    try:
        file_mapping = {
            'processed': 'processed_churn.csv',
            'predictions': 'predicted_churn.csv',
            'customer_data': 'processed_customer_data.csv'
        }
        
        filename = file_mapping.get(data_type)
        if not filename:
            return jsonify({'error': 'Invalid data type'}), 400
        
        file_path = os.path.join(DATA_DIR, filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True, download_name=filename)
        else:
            return jsonify({'error': 'File not found'}), 404
            
    except Exception as e:
        logger.error(f"Download error: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print("🚀 Starting Flask Churn Prediction Web App...")
    print("📊 Dashboard: http://localhost:5000")
    print("🔧 Models loaded:", "✅" if predictor.model else "❌")
    
    app.run(debug=True, host='0.0.0.0', port=5001) 