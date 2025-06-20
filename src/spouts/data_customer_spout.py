import os
import csv
import time
from streamparse.spout import Spout

class DataCustomerSpout(Spout):
    outputs = ['customerID', 'value']  

    def initialize(self, conf, context):
        # Use correct path resolution
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        self.input_file = os.path.join(base_path, "data/WA_Fn-UseC_-Telco-Customer-Churn.csv")
        
        self.current_row = 0
        self.total_rows = 0
        self.cycle_count = 0
        self.sleep_time = 0.1  # Fast processing
        
        # Count total rows first
        try:
            with open(self.input_file, mode="r", encoding="utf-8") as f:
                self.total_rows = sum(1 for line in f) - 1  # Exclude header
            self.log(f"Total rows to process: {self.total_rows}")
        except IOError as e:
            self.log(f"Error counting rows: {e}")
            self.total_rows = 0

        self._reset_file()

    def _reset_file(self):
        """Reset file pointer to beginning"""
        try:
            if hasattr(self, 'file'):
                self.file.close()
            
            self.file = open(self.input_file, mode="r", encoding="utf-8")
            self.reader = csv.reader(self.file)
            self.header = next(self.reader)  # Skip header
            self.current_row = 0
            self.cycle_count += 1
            self.log(f"Starting cycle #{self.cycle_count}, processing {self.total_rows} rows")
            
        except IOError as e:
            self.log(f"Error opening file: {e}")

    def next_tuple(self):
        try:
            row = next(self.reader)
            customerID = row[0]  
            value = row[1:]
            
            # Add metadata for tracking
            data_with_meta = {
                'customerID': customerID,
                'data': value,
                'row_number': self.current_row + 1,
                'cycle': self.cycle_count,
                'timestamp': time.time(),
                'total_rows': self.total_rows
            }
            
            self.emit([customerID, data_with_meta])  
            self.current_row += 1
            
            # Log progress every 100 rows
            if self.current_row % 100 == 0:
                progress = (self.current_row / self.total_rows) * 100
                self.log(f"Cycle {self.cycle_count}: Processed {self.current_row}/{self.total_rows} rows ({progress:.1f}%)")

        except StopIteration:
            # Finished current cycle, restart for continuous processing
            self.log(f"Completed cycle #{self.cycle_count} - processed {self.current_row} rows")
            self._reset_file()
            
            # Add small delay between cycles to prevent overwhelming
            time.sleep(2)
            return

        # Control processing speed
        time.sleep(self.sleep_time)

    def declare_output_fields(self):
        return ('customerID', 'value')  
