import os
import csv
from streamparse.spout import Spout

class DataCustomerSpoutWithStats(Spout):
    outputs = ['customerID', 'value'] 

    def initialize(self, conf, context):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        self.input_file = os.path.join(base_path, "data/WA_Fn-UseC_-Telco-Customer-Churn.csv")

        try:
            self.file = open(self.input_file, mode="r", encoding="utf-8")
            self.reader = csv.reader(self.file)
            self.header = next(self.reader)  
            self.finished = False
        except IOError as e:
            self.log(f"Lỗi khi mở file: {e}")

    def next_tuple(self):
        if self.finished:
            self.sleep(1)  
            return

        try:
            row = next(self.reader)
            customerID = row[0]  
            value = row[1:]  
            self.emit([customerID, value])  
        except StopIteration:
            self.finished = True  
            self.file.close()  
            self.sleep(1) 

    def declare_output_fields(self):
        return ('customerID', 'value')  
