import os
import csv
from streamparse.bolt import Bolt

class CustomerSearchBolt(Bolt):
    def initialize(self, conf, context):
        self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
        self.output_yes_file = os.path.join(self.base_path, "data/data_for_searching_yes.csv")
        self.output_no_file = os.path.join(self.base_path, "data/data_for_searching_no.csv")

        self.file_yes = open(self.output_yes_file, mode="a", newline="")
        self.file_no = open(self.output_no_file, mode="a", newline="")

        self.writer_yes = csv.writer(self.file_yes)
        self.writer_no = csv.writer(self.file_no)

        if os.stat(self.output_yes_file).st_size == 0:
            self.writer_yes.writerow(["customerID", "value"])

        if os.stat(self.output_no_file).st_size == 0:
            self.writer_no.writerow(["customerID", "value"])

    def process(self, tup):
        try:
            customerID, value = tup
            churn = value[-1]  # Lấy giá trị của trường 'Churn'

            # Phân vùng theo giá trị Churn
            if churn == 'Yes':
                self.writer_yes.writerow([customerID, value])  # Lưu vào file data_for_searching_yes.csv
                self.file_yes.flush()
            elif churn == 'No':
                self.writer_no.writerow([customerID, value])  # Lưu vào file data_for_searching_no.csv
                self.file_no.flush()

        except Exception as e:
            self.log(f"Error processing tuple: {e}")

    def cleanup(self):
        if hasattr(self, 'file_yes') and self.file_yes:
            self.file_yes.close()
        if hasattr(self, 'file_no') and self.file_no:
            self.file_no.close()
