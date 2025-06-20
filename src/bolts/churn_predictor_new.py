import os
import csv
import joblib
import numpy as np
import pandas as pd
from streamparse.bolt import Bolt

class ChurnPredictorNewBolt(Bolt):
    def initialize(self, conf, context):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        
        model_path = os.path.join(base_path, "models/logistic_model_new.pkl")
        preprocessor_path = os.path.join(base_path, "models/preprocessor_new.pkl")

        self.model = joblib.load(model_path)
        self.preprocessor = joblib.load(preprocessor_path)

        self.csv_file = os.path.join(base_path, "data/predicted_churn_new.csv")

        try:
            self.file = open(self.csv_file, mode="a", newline="")
            self.writer = csv.writer(self.file)

            if os.stat(self.csv_file).st_size == 0:
                self.writer.writerow(["TotalCharges", "MonthlyCharges", "Predicted_Churn", "Probability"])

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
            X_processed = self.preprocessor.transform(data)

            # Dự đoán Churn với mô hình logistic regression
            prediction = int(self.model.predict(X_processed)[0])
            probability = self.model.predict_proba(X_processed)[0][1]  # Prob of Yes

            # Lưu kết quả vào CSV
            self.writer.writerow([TotalCharges, MonthlyCharges, prediction, probability])
            self.file.flush()  

            self.emit([TotalCharges, MonthlyCharges, prediction, probability])

        except Exception as e:
            self.log(f"Lỗi dự đoán: {e}")

    def cleanup(self):
        if hasattr(self, 'file') and self.file:
            self.file.close() 