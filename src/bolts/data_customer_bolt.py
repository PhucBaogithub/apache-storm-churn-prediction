import os
import csv
from streamparse.bolt import Bolt

class DataCustomerBolt(Bolt):
    def initialize(self, conf, context):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
        self.output_file = os.path.join(base_path, "data/processed_customer_data.csv")

        try:
            self.file = open(self.output_file, mode="a", encoding="utf-8", newline="")
            self.writer = csv.writer(self.file)

            if os.stat(self.output_file).st_size == 0:
                self.writer.writerow(["customerID", "value"])

        except IOError as e:
            self.log(f"Lỗi mở file: {e}")

    def process(self, tup):
        try:
            customerID, value = tup

            # Ghi vào file CSV
            self.writer.writerow([customerID, ','.join(value)])
            self.file.flush()  

            self.emit([customerID, value])
            self.log(f"Ghi dữ liệu: {customerID}, {value}")

        except Exception as e:
            self.log(f"Lỗi trong quá trình ghi dữ liệu: {e}")

    def cleanup(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()
