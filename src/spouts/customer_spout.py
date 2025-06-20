import os
import csv
from streamparse.spout import Spout

class CustomerSpout(Spout):
    outputs = ['key', 'value']  

    def initialize(self, stormconf, context):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        file_path = os.path.join(base_path, "data/WA_Fn-UseC_-Telco-Customer-Churn.csv")

        self.file = open(file_path, mode="r", encoding="utf-8")
        self.reader = csv.DictReader(self.file)
        self.finished = False  

    def next_tuple(self):
        if self.finished:
            return  

        try:
            row = next(self.reader)

            total_charges = row.get('TotalCharges', "0.0").strip()
            monthly_charges = row.get('MonthlyCharges', "0.0").strip()
            churn = row.get('Churn', "No").strip()  # Lấy giá trị của trường Churn

            # Chuyển đổi kiểu dữ liệu
            total_charges = float(total_charges) if total_charges else 0.0
            monthly_charges = float(monthly_charges)

            # Tạo tuple với key là (TotalCharges, MonthlyCharges) và value là Churn
            key = (total_charges, monthly_charges)
            value = churn
            self.emit([key, value])  

        except StopIteration:
            self.finished = True  

    def cleanup(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()  
