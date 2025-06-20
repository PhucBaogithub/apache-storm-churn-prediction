import os
import csv
from streamparse.bolt import Bolt

class ChurnDataBolt(Bolt):
    def initialize(self, conf, context):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        self.output_file = os.path.join(base_path, "data/processed_churn.csv")

        try:
            self.file = open(self.output_file, mode="a", newline="")
            self.writer = csv.writer(self.file)

            if os.stat(self.output_file).st_size == 0:
                self.writer.writerow(["TotalCharges", "MonthlyCharges", "Churn"])

        except IOError as e:
            self.log(f"Lỗi mở file: {e}")

    def process(self, tup):
        try:
            key, value = tup
            total_charges, monthly_charges = key
            churn = value

            # Ghi vào file CSV
            self.writer.writerow([total_charges, monthly_charges, churn])
            self.file.flush() 

            self.log(f" Ghi dữ liệu: {total_charges}, {monthly_charges}, {churn}")

        except Exception as e:
            self.log(f"Lỗi trong quá trình ghi dữ liệu: {e}")

    def cleanup(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()
            self.log("Đã đóng tệp CSV.")
