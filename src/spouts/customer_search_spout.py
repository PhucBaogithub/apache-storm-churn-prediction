import os
import csv
from streamparse.spout import Spout

class CustomerSearchSpout(Spout):
    outputs = ['customerID', 'value']

    def initialize(self, stormconf, context):
        self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
        self.file_path = os.path.join(self.base_path, "data/WA_Fn-UseC_-Telco-Customer-Churn.csv")
        self.finished = False
        self.file = open(self.file_path, mode="r", encoding="utf-8")
        self.reader = csv.DictReader(self.file)

    def next_tuple(self):
        if self.finished:
            return

        try:
            row = next(self.reader)
            customerID = row['customerID']
            # Lấy tất cả các trường còn lại làm value
            value = row[1:]
            
            # Emit key-value, phân vùng theo churn
            self.emit([customerID, value])  # Phát tuple có key là customerID và value là các trường còn lại

        except StopIteration:
            self.finished = True

    def cleanup(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()
