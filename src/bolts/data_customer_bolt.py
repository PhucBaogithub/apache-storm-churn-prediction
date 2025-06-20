import os
import csv
import time
from datetime import datetime
from streamparse.bolt import Bolt

class DataCustomerBolt(Bolt):
    def initialize(self, conf, context):
        # Use correct path resolution
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        self.output_file = os.path.join(base_path, "data/processed_customer_data.csv")
        
        self.processed_count = 0
        self.batch_size = 100
        self.last_flush = time.time()
        self.flush_interval = 5  # Flush every 5 seconds
        
        self._initialize_output_file()

    def _initialize_output_file(self):
        """Initialize output file with headers"""
        try:
            # Check if file exists and is empty
            file_exists = os.path.exists(self.output_file)
            is_empty = not file_exists or os.stat(self.output_file).st_size == 0
            
            self.file = open(self.output_file, mode="a", encoding="utf-8", newline="")
            self.writer = csv.writer(self.file)

            if is_empty:
                # Write header with comprehensive fields
                header = [
                    "customerID", "gender", "SeniorCitizen", "Partner", "Dependents", 
                    "tenure", "PhoneService", "MultipleLines", "InternetService",
                    "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
                    "StreamingTV", "StreamingMovies", "Contract", "PaperlessBilling",
                    "PaymentMethod", "MonthlyCharges", "TotalCharges", "Churn",
                    "processed_timestamp", "cycle", "row_number"
                ]
                self.writer.writerow(header)
                self.file.flush()
                self.log("Initialized output file with headers")

        except IOError as e:
            self.log(f"Error opening output file: {e}")

    def process(self, tup):
        try:
            customerID, data_with_meta = tup
            
            # Extract data based on whether it's metadata format or simple format
            if isinstance(data_with_meta, dict):
                # New format with metadata
                customer_data = data_with_meta.get('data', [])
                cycle = data_with_meta.get('cycle', 1)
                row_number = data_with_meta.get('row_number', 0)
                timestamp = data_with_meta.get('timestamp', time.time())
            else:
                # Legacy format - simple list
                customer_data = data_with_meta
                cycle = 1
                row_number = self.processed_count
                timestamp = time.time()

            # Create processed row with timestamp
            processed_timestamp = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
            
            # Build complete row
            row_data = [customerID] + customer_data + [processed_timestamp, cycle, row_number]
            
            # Write to CSV
            self.writer.writerow(row_data)
            self.processed_count += 1

            # Periodic flush to ensure data is written
            current_time = time.time()
            if (self.processed_count % self.batch_size == 0 or 
                current_time - self.last_flush > self.flush_interval):
                self.file.flush()
                self.last_flush = current_time

            # Emit for further processing
            enriched_data = {
                'customerID': customerID,
                'data': customer_data,
                'processed_timestamp': processed_timestamp,
                'cycle': cycle,
                'row_number': row_number,
                'processed_count': self.processed_count
            }
            
            self.emit([customerID, enriched_data])
            
            # Log progress every 50 records
            if self.processed_count % 50 == 0:
                self.log(f"Processed {self.processed_count} records (Cycle {cycle}, Row {row_number})")

        except Exception as e:
            self.log(f"Error processing data: {e}")
            # Still try to emit something to prevent topology failure
            self.emit([customerID, data_with_meta])

    def cleanup(self):
        if hasattr(self, 'file') and self.file:
            self.file.flush()
            self.file.close()
            self.log(f"Cleanup completed. Total processed: {self.processed_count}")
