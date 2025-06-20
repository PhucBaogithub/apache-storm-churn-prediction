#!/usr/bin/env python
"""
Script để simulate real Storm spout/bolt outputs 
Tạo files giống như khi Storm topology thực sự chạy và xuất data
"""
import os
import sys
import pandas as pd
import csv
import joblib
import numpy as np
import time
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def simulate_churn_spout_output():
    """Simulate ChurnDataSpout - đọc và stream dữ liệu"""
    print("🔄 Simulating ChurnDataSpout...")
    
    source_file = "data/WA_Fn-UseC_-Telco-Customer-Churn.csv"
    if not os.path.exists(source_file):
        print("❌ Source CSV file not found!")
        return False
    
    df = pd.read_csv(source_file)
    print(f"✅ ChurnDataSpout: Loaded {len(df)} records from CSV")
    
    # Simulate streaming output - write to spout output file
    spout_output = "data/spout_raw_output.csv"
    sample_data = df.sample(n=min(100, len(df))).copy()
    sample_data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sample_data['spout_id'] = 'churn_data_spout'
    
    sample_data.to_csv(spout_output, index=False)
    print(f"✅ ChurnDataSpout: Written {len(sample_data)} records to {spout_output}")
    
    return sample_data

def simulate_churn_data_bolt_output(spout_data):
    """Simulate ChurnDataBolt - xử lý và clean dữ liệu"""
    print("🔄 Simulating ChurnDataBolt...")
    
    if spout_data is None:
        return None
    
    # Process data like ChurnDataBolt
    processed_data = spout_data[['customerID', 'TotalCharges', 'MonthlyCharges', 'Churn']].copy()
    
    # Convert TotalCharges to numeric (like bolt does)
    processed_data['TotalCharges'] = pd.to_numeric(processed_data['TotalCharges'], errors='coerce')
    processed_data['TotalCharges'] = processed_data['TotalCharges'].fillna(0)
    
    # Add processing metadata
    processed_data['processed_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    processed_data['bolt_id'] = 'churn_data_bolt'
    processed_data['processing_status'] = 'SUCCESS'
    
    # Output processed data
    bolt_output = "data/bolt_processed_output.csv"
    processed_data.to_csv(bolt_output, index=False)
    print(f"✅ ChurnDataBolt: Processed {len(processed_data)} records to {bolt_output}")
    
    return processed_data

def simulate_churn_predictor_bolt_output(processed_data):
    """Simulate ChurnPredictorBolt - ML predictions"""
    print("🔄 Simulating ChurnPredictorBolt...")
    
    if processed_data is None:
        return None
    
    # Load ML model like the bolt does
    try:
        model = joblib.load("models/logistic_model_new.pkl")
        preprocessor = joblib.load("models/preprocessor_new.pkl")
        print("✅ ChurnPredictorBolt: ML models loaded")
    except:
        print("⚠️ ChurnPredictorBolt: Using dummy predictions (models not found)")
        model = None
        preprocessor = None
    
    predictions = []
    
    for idx, row in processed_data.iterrows():
        customer_id = row['customerID']
        total_charges = row['TotalCharges']
        monthly_charges = row['MonthlyCharges']
        actual_churn = row['Churn']
        
        if model and preprocessor:
            # Real ML prediction
            try:
                data = pd.DataFrame([{
                    'TotalCharges': total_charges,
                    'MonthlyCharges': monthly_charges
                }])
                X_processed = preprocessor.transform(data)
                prediction = int(model.predict(X_processed)[0])
                probability = model.predict_proba(X_processed)[0][1]
            except:
                # Fallback to dummy prediction
                prediction = 1 if total_charges > 2000 else 0
                probability = 0.65 if prediction else 0.35
        else:
            # Dummy prediction logic
            prediction = 1 if total_charges > 2000 else 0
            probability = 0.65 if prediction else 0.35
        
        predictions.append({
            'customerID': customer_id,
            'TotalCharges': total_charges,
            'MonthlyCharges': monthly_charges,
            'Predicted_Churn': prediction,
            'Predicted_Probability': probability,
            'Actual_Churn': actual_churn,
            'prediction_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bolt_id': 'churn_predictor_bolt',
            'model_version': 'logistic_v1.0'
        })
    
    pred_df = pd.DataFrame(predictions)
    
    # Output predictions
    predictor_output = "data/bolt_predictions_output.csv"
    pred_df.to_csv(predictor_output, index=False)
    print(f"✅ ChurnPredictorBolt: Generated {len(pred_df)} predictions to {predictor_output}")
    
    return pred_df

def simulate_customer_search_spout_output():
    """Simulate CustomerSearchSpout - tìm kiếm khách hàng"""
    print("🔄 Simulating CustomerSearchSpout...")
    
    source_file = "data/WA_Fn-UseC_-Telco-Customer-Churn.csv"
    if not os.path.exists(source_file):
        return None
    
    df = pd.read_csv(source_file)
    
    # Simulate search results
    search_results = df.sample(n=50).copy()
    search_results['search_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    search_results['search_query'] = 'sample_search'
    search_results['spout_id'] = 'customer_search_spout'
    
    search_output = "data/search_spout_output.csv"
    search_results.to_csv(search_output, index=False)
    print(f"✅ CustomerSearchSpout: Generated {len(search_results)} search results to {search_output}")
    
    return search_results

def create_final_processed_files():
    """Tạo các files cuối cùng mà UI cần"""
    print("🔄 Creating final processed files for UI...")
    
    # Copy bolt outputs to expected UI paths
    files_to_copy = [
        ("data/bolt_processed_output.csv", "data/processed_churn.csv"),
        ("data/bolt_predictions_output.csv", "data/predicted_churn.csv"),
        ("data/search_spout_output.csv", "data/customer_search_results.csv")
    ]
    
    for source, dest in files_to_copy:
        if os.path.exists(source):
            df = pd.read_csv(source)
            df.to_csv(dest, index=False)
            print(f"✅ Copied {source} → {dest} ({len(df)} records)")
    
    # Create processed_customer_data.csv for UI
    if os.path.exists("data/bolt_processed_output.csv"):
        df = pd.read_csv("data/bolt_processed_output.csv")
        customer_data = df[['customerID', 'TotalCharges', 'MonthlyCharges', 'Churn']].copy()
        customer_data.to_csv("data/processed_customer_data.csv", index=False)
        print(f"✅ Created processed_customer_data.csv ({len(customer_data)} records)")

def main():
    """Chạy simulation đầy đủ Storm topology"""
    print("🚀 SIMULATING FULL STORM TOPOLOGY EXECUTION")
    print("=" * 50)
    
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    # Simulate Storm topology flow
    print("\n1️⃣ SPOUT LAYER:")
    spout_data = simulate_churn_spout_output()
    search_data = simulate_customer_search_spout_output()
    
    print("\n2️⃣ BOLT LAYER:")
    processed_data = simulate_churn_data_bolt_output(spout_data)
    prediction_data = simulate_churn_predictor_bolt_output(processed_data)
    
    print("\n3️⃣ FINAL OUTPUT:")
    create_final_processed_files()
    
    print("\n🎉 STORM TOPOLOGY SIMULATION COMPLETE!")
    print("\n📂 Generated Output Files:")
    
    output_files = [
        "data/spout_raw_output.csv",
        "data/bolt_processed_output.csv", 
        "data/bolt_predictions_output.csv",
        "data/search_spout_output.csv",
        "data/processed_churn.csv",
        "data/predicted_churn.csv",
        "data/customer_search_results.csv",
        "data/processed_customer_data.csv"
    ]
    
    for file_path in output_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            df = pd.read_csv(file_path)
            print(f"  ✅ {file_path} ({len(df)} records, {size} bytes)")
        else:
            print(f"  ❌ {file_path} (missing)")
    
    print(f"\n🕒 Simulation completed at: {datetime.now()}")
    print("📌 Files ready for Streamlit UI consumption!")

if __name__ == "__main__":
    main() 