import os
import csv
import joblib
import numpy as np
import pandas as pd
from streamparse.bolt import Bolt

class ChurnPredictorBolt(Bolt):
    def initialize(self, conf, context):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        
        model_path = os.path.join(base_path, "models/logistic_mbgd_model.pkl")
        preprocessor_path = os.path.join(base_path, "models/preprocessor.pkl")

        self.model = joblib.load(model_path)
        self.preprocessor = joblib.load(preprocessor_path)

        self.csv_file = os.path.join(base_path, "data/predicted_churn.csv")

        try:
            self.file = open(self.csv_file, mode="a", newline="")
            self.writer = csv.writer(self.file)

            if os.stat(self.csv_file).st_size == 0:
                self.writer.writerow(["TotalCharges", "MonthlyCharges", "Predicted_Churn"])

        except IOError as e:
            self.log(f"Lỗi mở file: {e}")

    def process(self, tup):
        try:
            TotalCharges, MonthlyCharges = tup

            data = pd.DataFrame([{
                'TotalCharges': TotalCharges,
                'MonthlyCharges': MonthlyCharges
            }])

            # Chuẩn hóa dữ liệu 
            X_processed = self.preprocessor.transform(data).flatten()  # Chuyển dữ liệu thành 1D
            X_processed = np.hstack(([1.0], X_processed))  # Thêm bias (hệ số chặn)

            # Dự đoán Churn với mô hình logistic regression
            y_pred_prob = 1 / (1 + np.exp(-np.dot(X_processed, self.model)))  # Công thức sigmoid
            prediction = int(y_pred_prob >= 0.5)  # Ngưỡng 0.5 để xác định dự đoán

            # Lưu kết quả vào CSV
            self.writer.writerow([TotalCharges, MonthlyCharges, prediction])
            self.file.flush()  

            self.emit([TotalCharges, MonthlyCharges, prediction])

        except Exception as e:
            self.log(f"Lỗi dự đoán: {e}")

    def cleanup(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()  
