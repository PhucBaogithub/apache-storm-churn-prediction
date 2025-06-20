import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import csv
from streamparse.bolt import Bolt

class DataCustomerBoltWithStats(Bolt):
    def initialize(self, conf, context):
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        self.output_file = os.path.join(base_path, "data/processed_customer_data.csv")

        self.file = open(self.output_file, mode="a", encoding="utf-8", newline="")
        self.writer = csv.writer(self.file)

        if os.stat(self.output_file).st_size == 0:
            self.writer.writerow(["customerID", "value"])

        self.data = []
        self.churned_customers = []

    def process(self, tup):
        try:
            customerID, value = tup
            self.writer.writerow([customerID, ','.join(value)])
            self.file.flush()  

            self.data.append(value)

            churn_status = value[-1]  
            if churn_status == 'Yes':  
                self.churned_customers.append(customerID)

        except Exception as e:
            self.log(f"Lỗi trong quá trình ghi dữ liệu: {e}")

    def cleanup(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()

        # Chuyển dữ liệu sang dataframe
        df = pd.DataFrame(self.data, columns=["Feature_" + str(i) for i in range(1, len(self.data[0]) + 1)])

        # Cập nhật lại tên cột cho dễ dàng truy cập
        df.columns = ["customerID", "gender", "SeniorCitizen", "Partner", "Dependents", "tenure", 
                      "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup", 
                      "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies", "Contract", 
                      "PaperlessBilling", "PaymentMethod", "MonthlyCharges", "TotalCharges", "Churn"]

        # Chuyển TotalCharges và MonthlyCharges thành kiểu số
        df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors='coerce')
        df["MonthlyCharges"] = pd.to_numeric(df["MonthlyCharges"], errors='coerce')

        # Tính toán thống kê cơ bản
        stats = df.describe()

        # Lưu thống kê vào file CSV
        stats_file = os.path.join(os.path.dirname(self.output_file), "statistics_summary.csv")
        stats.to_csv(stats_file)
        self.log(f"Đã lưu thống kê vào file: {stats_file}")

        # Biểu đồ pie cho khách hàng rời đi
        churned_count = len(self.churned_customers)
        not_churned_count = len(self.data) - churned_count

        labels = ['Khách hàng rời đi', 'Khách hàng không rời đi']
        sizes = [churned_count, not_churned_count]
        explode = (0.1, 0)

        plt.figure(figsize=(7, 7))
        plt.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True, startangle=140)
        plt.title("Tỷ lệ khách hàng rời đi vs không rời đi")
        plt.axis('equal')
        plt.savefig(os.path.join(os.path.dirname(self.output_file), "churned_customers_piechart.png"))
        plt.close()

        # Tính giá trị trung bình của TotalCharges và MonthlyCharges theo Churn
        churn_grouped = df.groupby('Churn')[['TotalCharges', 'MonthlyCharges']].mean()

        # Vẽ biểu đồ cho trung bình TotalCharges và MonthlyCharges theo Churn
        churn_grouped.plot(kind='bar', figsize=(10, 6), color=["orange", "yellow"])
        plt.title("Trung bình TotalCharges và MonthlyCharges theo Churn")
        plt.ylabel("Giá trị trung bình ($)")
        plt.xlabel("Churn (0: Không, 1: Có)")
        plt.legend(["TotalCharges", "MonthlyCharges"])
        plt.savefig(os.path.join(os.path.dirname(self.output_file), "average_charges_by_churn.png"))
        plt.close()

        # Vẽ biểu đồ phân phối khách hàng theo TotalCharges (có phân loại Churn)
        plt.figure(figsize=(12, 7))
        sns.histplot(df, x='TotalCharges', hue='Churn', multiple="stack", kde=True, bins=30, palette="Set2")
        plt.title("Phân phối khách hàng theo TotalCharges (có phân loại Churn)")
        plt.xlabel("TotalCharges ($)")
        plt.ylabel("Số lượng khách hàng")
        plt.legend(title="Churn", labels=["Không rời đi", "Rời đi"])
        plt.savefig(os.path.join(os.path.dirname(self.output_file), "customer_distribution_by_totalcharges.png"))
        plt.close()
