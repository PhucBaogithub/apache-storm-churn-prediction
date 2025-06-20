import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
import seaborn as sns
import joblib

# Bước 1: Đọc dữ liệu từ file CSV
df = pd.read_csv("data/WA_Fn-UseC_-Telco-Customer-Churn.csv")

df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors='coerce')
df = df.dropna(subset=["TotalCharges", "Churn"])
df["Churn"] = (df["Churn"] == "Yes").astype(int)

# Chọn đặc trưng và nhãn (chỉ sử dụng 2 trường: MonthlyCharges và TotalCharges)
features = ['TotalCharges', 'MonthlyCharges']
X = df[features]
y = df["Churn"].values

# Xử lý dữ liệu số
numeric_features = ['TotalCharges', 'MonthlyCharges']
numeric_transformer = StandardScaler()

# Không cần xử lý dữ liệu phân loại vì không có trường phân loại nào
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features)
    ])

# Tiền xử lý dữ liệu
X_processed = preprocessor.fit_transform(X)

# Thêm hệ số chặn
X_processed = np.hstack((np.ones((X_processed.shape[0], 1)), X_processed))  # Thêm hệ số chặn

# Chia dữ liệu thành tập huấn luyện và kiểm tra
X_train, X_test, y_train, y_test = train_test_split(X_processed, y, test_size=0.2, random_state=42, stratify=y)

# Khởi tạo tham số
np.random.seed(42)
theta = np.zeros(X_train.shape[1])
lr = 0.00009  # Tăng tốc độ học để cập nhật nhanh hơn
num_iter = 5000  # Tăng số lần lặp
batch_size = 128  # Tăng batch size để giảm nhiễu
momentum = 0.9  # Giữ lại momentum để tối ưu
lambda_reg = 0.01  # Regularization để tránh overfitting
batch_losses = []

# Hàm sigmoid
def sigmoid_function(z):
    return 1 / (1 + np.exp(-z))

# Hàm mất mát (thêm regularization L2)
def loss_function(y_hat, y, theta, lambda_reg=0.01):
    loss = (-y*np.log(y_hat) - (1 - y)*np.log(1 - y_hat)).mean()
    reg = (lambda_reg / 2) * np.sum(theta**2)  # Ridge Regularization
    return loss + reg

# Huấn luyện với Mini-Batch Gradient Descent có Momentum và Early Stopping
N = X_train.shape[0]
velocity = np.zeros_like(theta)
early_stopping_threshold = 50  # Số lần không cải thiện loss trước khi dừng
best_loss = float('inf')
no_improvement = 0

for epoch in range(num_iter):
    shuffled_indices = np.random.permutation(N)
    X_train = X_train[shuffled_indices]
    y_train = y_train[shuffled_indices]
    
    for i in range(0, N, batch_size):
        X_batch = X_train[i:i+batch_size]
        y_batch = y_train[i:i+batch_size]
        
        y_hat = sigmoid_function(np.dot(X_batch, theta))
        gradient = (np.dot(X_batch.T, (y_hat - y_batch)) / batch_size) + lambda_reg * theta  # Thêm L2
        
        # Cập nhật theta bằng momentum
        velocity = momentum * velocity - lr * gradient
        theta += velocity
    
    # Tính loss
    current_loss = loss_function(y_hat, y_batch, theta)
    batch_losses.append(current_loss)
    
    # Kiểm tra Early Stopping
    if current_loss < best_loss:
        best_loss = current_loss
        no_improvement = 0
    else:
        no_improvement += 1
    
    if no_improvement >= early_stopping_threshold:
        print(f"Stopping early at epoch {epoch} due to no improvement.")
        break

# Vẽ biểu đồ mất mát
plt.plot(batch_losses)
plt.xlabel("Iterations")
plt.ylabel("Loss")
plt.title("Loss over iterations")
plt.show()

# Đánh giá trên tập kiểm tra
y_pred_prob = sigmoid_function(np.dot(X_test, theta))
y_pred = np.round(y_pred_prob)

print(f'Accuracy: {accuracy_score(y_test, y_pred):.4f}')
print(classification_report(y_test, y_pred))

sns.heatmap(confusion_matrix(y_test, y_pred), annot=True, fmt='d', cmap='Blues')
plt.xlabel("Predicted")
plt.ylabel("Actual")
plt.title("Confusion Matrix")
plt.show()

# Lưu mô hình và scaler
joblib.dump(theta, "models/logistic_mbgd_model.pkl")
joblib.dump(preprocessor, "models/preprocessor.pkl")
print("Mô hình và bộ xử lý đã được lưu thành công!")
