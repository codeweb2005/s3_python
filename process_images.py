import boto3
import os
from PIL import Image

# Khai báo tên bucket input & output
BUCKET_INPUT = 's3-upload-912005'
BUCKET_OUTPUT = 'bucket-output-912005'
REGION = 'ap-southeast-1'

s3 = boto3.client('s3', region_name=REGION)

def list_objects_to_process():
    """
    Liệt kê tất cả object trong bucket input ở prefix 'uploads/'
    (Bạn có thể tinh chỉnh prefix tùy theo folder structure)
    """
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_INPUT, Prefix='uploads/')
    
    keys = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Bỏ qua thư mục (Key ending with '/')
                if not key.endswith('/'):
                    keys.append(key)
    return keys

def download_image(key, download_path):
    """
    Tải file xuống local
    """
    os.makedirs(os.path.dirname(download_path), exist_ok=True)
    s3.download_file(BUCKET_INPUT, key, download_path)

def process_image(local_path, local_out_path):
    """
    Mở ảnh, thực hiện thao tác (ví dụ resize), lưu kết quả
    """
    img = Image.open(local_path)
    # Ví dụ: resize thành 800x800 tối đa, giữ tỷ lệ
    img.thumbnail((800, 800))
    img.save(local_out_path)

def upload_image(key, local_out_path):
    """
    Upload file đã xử lý lên S3 output, 
    ví dụ đổi prefix từ 'uploads/' thành 'processed/'
    """
    # Chuyển đổi key ví dụ:
    new_key = key.replace('uploads/', 'processed/')
    s3.upload_file(local_out_path, BUCKET_OUTPUT, new_key)
    return new_key

def main():
    keys = list_objects_to_process()
    for key in keys:
        print(f"Processing key: {key}")
        filename = os.path.basename(key)
        local_input = f"/tmp/{filename}"
        local_output = f"/tmp/processed-{filename}"
        
        # 1) Download
        download_image(key, local_input)
        
        # 2) Xử lý ảnh
        process_image(local_input, local_output)
        
        # 3) Upload kết quả
        out_key = upload_image(key, local_output)
        print(f"Uploaded processed image to: {out_key}")
        
        # 4) (Tùy chọn) Xoá file gốc nếu không cần giữ
        # s3.delete_object(Bucket=BUCKET_INPUT, Key=key)
    
    print("All images processed.")

if __name__ == "__main__":
    main()
