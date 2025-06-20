#!/bin/bash

echo "📤 Pushing to GitHub..."
echo "========================"

# Thêm remote repository (thay YOUR_USERNAME bằng username GitHub của bạn)
echo "⚠️  Thay YOUR_USERNAME bằng username GitHub thực tế của bạn"
echo "Ví dụ: git remote add origin https://github.com/phucbao/storm-churn-prediction.git"
echo ""

read -p "Nhập GitHub username của bạn: " username

if [ -z "$username" ]; then
    echo "❌ Username không được để trống!"
    exit 1
fi

# Set remote origin
git remote add origin https://github.com/$username/storm-churn-prediction.git

# Push to GitHub
git branch -M main
git push -u origin main

echo "✅ Project đã được đưa lên GitHub thành công!"
echo "🌐 Truy cập: https://github.com/$username/storm-churn-prediction" 