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

@app.route('/api/statistics')
def get_statistics_data():
    """Get statistics summary data"""
    try:
        stats_file = os.path.join(DATA_DIR, 'statistics_summary.csv')
        if os.path.exists(stats_file):
            df = pd.read_csv(stats_file)
            
            # Convert DataFrame to a more readable format
            stats_data = []
            for col in df.columns:
                stats_data.append({
                    'field': col,
                    'count': df[col].iloc[0] if len(df) > 0 else 0,
                    'unique': df[col].iloc[1] if len(df) > 1 else 0,
                    'top_value': df[col].iloc[2] if len(df) > 2 else '',
                    'frequency': df[col].iloc[3] if len(df) > 3 else 0
                })
            
            return jsonify({
                'success': True,
                'data': stats_data,
                'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Statistics file not found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error reading statistics: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/search_customers', methods=['POST'])
def search_customers():
    """Search customers from the main dataset"""
    try:
        data = request.get_json()
        search_query = data.get('query', '').strip().lower()
        search_field = data.get('field', 'all')
        limit = int(data.get('limit', 50))
        
        # Load customer data
        customer_file = os.path.join(DATA_DIR, 'WA_Fn-UseC_-Telco-Customer-Churn.csv')
        if not os.path.exists(customer_file):
            return jsonify({
                'success': False,
                'error': 'Customer data file not found'
            }), 404
        
        df = pd.read_csv(customer_file)
        
        # Filter data based on search query
        if search_query and search_field != 'all':
            if search_field in df.columns:
                # Search in specific field
                mask = df[search_field].astype(str).str.lower().str.contains(search_query, na=False)
                filtered_df = df[mask]
            else:
                return jsonify({
                    'success': False,
                    'error': f'Field {search_field} not found'
                }), 400
        elif search_query:
            # Search in all text fields
            text_columns = ['customerID', 'gender', 'Partner', 'Dependents', 'PhoneService', 
                          'MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup',
                          'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies',
                          'Contract', 'PaperlessBilling', 'PaymentMethod', 'Churn']
            
            mask = pd.Series([False] * len(df))
            for col in text_columns:
                if col in df.columns:
                    mask |= df[col].astype(str).str.lower().str.contains(search_query, na=False)
            
            filtered_df = df[mask]
        else:
            # No search query, return recent records
            filtered_df = df.tail(limit)
        
        # Limit results
        result_df = filtered_df.head(limit)
        
        # Add search metadata
        result_data = result_df.to_dict('records')
        for record in result_data:
            record['search_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record['search_query'] = search_query
        
        return jsonify({
            'success': True,
            'data': result_data,
            'total_found': len(filtered_df),
            'showing': len(result_data),
            'search_query': search_query,
            'search_field': search_field,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
    except Exception as e:
        logger.error(f"Customer search error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/customer_search_results')
def get_customer_search_results():
    """Get saved customer search results"""
    try:
        search_file = os.path.join(DATA_DIR, 'customer_search_results.csv')
        if os.path.exists(search_file):
            df = pd.read_csv(search_file)
            
            # Get latest 100 records
            latest_data = df.tail(100).to_dict('records')
            
            return jsonify({
                'success': True,
                'data': latest_data,
                'total_records': len(df),
                'last_updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Customer search results file not found'
            }), 404
            
    except Exception as e:
        logger.error(f"Error reading customer search results: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/charts')
def get_available_charts():
    """Get list of available chart images"""
    try:
        charts = []
        chart_files = {
            'customer_distribution_by_totalcharges.png': 'Customer Distribution by Total Charges',
            'average_charges_by_churn.png': 'Average Charges by Churn Status',
            'churned_customers_piechart.png': 'Churned Customers Distribution'
        }
        
        for filename, title in chart_files.items():
            file_path = os.path.join(DATA_DIR, filename)
            if os.path.exists(file_path):
                charts.append({
                    'filename': filename,
                    'title': title,
                    'url': f'/api/chart/{filename}'
                })
        
        return jsonify({
            'success': True,
            'charts': charts
        })
        
    except Exception as e:
        logger.error(f"Error getting charts: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/chart/<filename>')
def serve_chart(filename):
    """Serve chart images"""
    try:
        # Security check: only allow specific chart files
        allowed_files = [
            'customer_distribution_by_totalcharges.png',
            'average_charges_by_churn.png', 
            'churned_customers_piechart.png'
        ]
        
        if filename not in allowed_files:
            return jsonify({'error': 'Chart not found'}), 404
            
        file_path = os.path.join(DATA_DIR, filename)
        if os.path.exists(file_path):
            return send_file(file_path, mimetype='image/png')
        else:
            return jsonify({'error': 'Chart file not found'}), 404
            
    except Exception as e:
        logger.error(f"Error serving chart: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter_customers', methods=['POST'])
def filter_customers():
    """Advanced customer filtering with multiple criteria"""
    try:
        data = request.get_json()
        filters = data.get('filters', {})
        limit = int(data.get('limit', 100))
        
        # Load customer data
        customer_file = os.path.join(DATA_DIR, 'WA_Fn-UseC_-Telco-Customer-Churn.csv')
        if not os.path.exists(customer_file):
            return jsonify({
                'success': False,
                'error': 'Customer data file not found'
            }), 404
        
        df = pd.read_csv(customer_file)
        
        # Apply filters
        filtered_df = df.copy()
        
        # Gender filter
        if filters.get('gender'):
            filtered_df = filtered_df[filtered_df['gender'] == filters['gender']]
            
        # Senior Citizen filter
        if filters.get('senior_citizen') is not None:
            filtered_df = filtered_df[filtered_df['SeniorCitizen'] == int(filters['senior_citizen'])]
            
        # Contract filter
        if filters.get('contract'):
            filtered_df = filtered_df[filtered_df['Contract'] == filters['contract']]
            
        # Internet Service filter
        if filters.get('internet_service'):
            filtered_df = filtered_df[filtered_df['InternetService'] == filters['internet_service']]
            
        # Churn filter
        if filters.get('churn'):
            filtered_df = filtered_df[filtered_df['Churn'] == filters['churn']]
            
        # Monthly Charges range filter
        if filters.get('monthly_charges_min') is not None:
            filtered_df = filtered_df[filtered_df['MonthlyCharges'] >= float(filters['monthly_charges_min'])]
        if filters.get('monthly_charges_max') is not None:
            filtered_df = filtered_df[filtered_df['MonthlyCharges'] <= float(filters['monthly_charges_max'])]
            
        # Total Charges range filter  
        if filters.get('total_charges_min') is not None:
            # Convert TotalCharges to numeric, handle non-numeric values
            filtered_df['TotalCharges'] = pd.to_numeric(filtered_df['TotalCharges'], errors='coerce')
            filtered_df = filtered_df[filtered_df['TotalCharges'] >= float(filters['total_charges_min'])]
        if filters.get('total_charges_max') is not None:
            filtered_df['TotalCharges'] = pd.to_numeric(filtered_df['TotalCharges'], errors='coerce')
            filtered_df = filtered_df[filtered_df['TotalCharges'] <= float(filters['total_charges_max'])]
            
        # Tenure range filter
        if filters.get('tenure_min') is not None:
            filtered_df = filtered_df[filtered_df['tenure'] >= int(filters['tenure_min'])]
        if filters.get('tenure_max') is not None:
            filtered_df = filtered_df[filtered_df['tenure'] <= int(filters['tenure_max'])]
        
        # Remove rows with NaN values
        filtered_df = filtered_df.dropna()
        
        # Limit results
        result_df = filtered_df.head(limit)
        
        # Add filter metadata
        result_data = result_df.to_dict('records')
        for record in result_data:
            record['filter_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Calculate summary statistics
        summary = {
            'total_found': len(filtered_df),
            'showing': len(result_data),
            'churn_rate': round((filtered_df['Churn'] == 'Yes').sum() / len(filtered_df) * 100, 2) if len(filtered_df) > 0 else 0,
            'avg_monthly_charges': round(filtered_df['MonthlyCharges'].mean(), 2) if len(filtered_df) > 0 else 0,
            'avg_total_charges': round(pd.to_numeric(filtered_df['TotalCharges'], errors='coerce').mean(), 2) if len(filtered_df) > 0 else 0
        }
        
        return jsonify({
            'success': True,
            'data': result_data,
            'summary': summary,
            'filters_applied': filters,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
    except Exception as e:
        logger.error(f"Customer filter error: {e}")
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
            'customer_data': 'processed_customer_data.csv',
            'statistics': 'statistics_summary.csv',
            'search_results': 'customer_search_results.csv'
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
    print("📊 Dashboard: http://localhost:5001")
    print("🔧 Models loaded:", "✅" if predictor.model else "❌")
    
    app.run(debug=True, host='0.0.0.0', port=5001) 