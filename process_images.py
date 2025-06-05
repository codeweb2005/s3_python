import boto3
import os
import sys
from PIL import Image
from datetime import datetime, timezone

# Tên bucket input/output (đã tạo trước đó)
BUCKET_INPUT  = 's3-upload-912005'
BUCKET_OUTPUT = 'bucket-output-912005'
PREFIX_UPLOAD = 'uploads/'       # Folder nơi ảnh được upload lên
PREFIX_PROCESSED = 'processed/'  # Folder nơi sẽ store ảnh nén

REGION = 'ap-southeast-1'

s3 = boto3.client('s3', region_name=REGION)

def find_latest_object(bucket, prefix):
    """
    Trả về key của object trong bucket/prefix có LastModified mới nhất (thời điểm UTC).
    Nếu không có object nào, trả về None.
    """
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    latest_key = None
    latest_time = datetime(1970, 1, 1, tzinfo=timezone.utc)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                # Bỏ qua nếu là folder (Key kết thúc bằng '/')
                key = obj['Key']
                if key.endswith('/'):
                    continue

                lm = obj['LastModified']
                if lm > latest_time:
                    latest_time = lm
                    latest_key = key

    return latest_key

def download_to_tmp(key):
    """
    Download object S3 về /tmp/<tên file>
    Trả về đường dẫn file local.
    """
    filename = os.path.basename(key)
    local_path = f"/tmp/{filename}"
    # Tạo thư mục /tmp nếu chưa có (thường AWS EC2 cho phép)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    print(f"Downloading s3://{BUCKET_INPUT}/{key} to {local_path} …")
    s3.download_file(BUCKET_INPUT, key, local_path)
    return local_path

def compress_image(input_path, output_path, quality=75, max_size=(800, 800)):
    """
    Mở file ảnh, resize và nén lại thành JPEG với độ "quality" (0–100).
    - quality thấp hơn → nén mạnh hơn, dung lượng file nhỏ hơn nhưng chất lượng kém hơn.
    - max_size là tuple (width, height) tối đa (giữ tỉ lệ).
    """
    img = Image.open(input_path)

    # Nếu muốn resize giữ tỉ lệ
    img.thumbnail(max_size)

    # Lưu ở định dạng JPEG với độ nén quality
    img = img.convert("RGB")  # Đảm bảo ảnh ở mode RGB (để save sang JPEG)
    img.save(output_path, "JPEG", quality=quality)
    print(f"Compressed and saved to {output_path} (quality={quality})")

def upload_to_s3(local_path, dest_key):
    """
    Upload file local lên S3-output tại dest_key
    """
    print(f"Uploading {local_path} to s3://{BUCKET_OUTPUT}/{dest_key} …")
    s3.upload_file(local_path, BUCKET_OUTPUT, dest_key)

def main():
    # 1) Tìm object mới nhất trong thư mục uploads/
    latest_key = find_latest_object(BUCKET_INPUT, PREFIX_UPLOAD)
    if latest_key is None:
        print("No objects found in bucket -> exit.")
        return

    print(f"Latest object to process: {latest_key}")

    # 2) Download về /tmp
    local_input = download_to_tmp(latest_key)

    # 3) Đặt tên local đầu ra (có thể khác hoặc giống tên gốc)
    filename = os.path.basename(latest_key)
    local_output = f"/tmp/compressed-{filename}"

    # 4) Compress image
    # Đặt quality=75 (bạn có thể chỉnh xuống 50–60 nếu muốn nén mạnh hơn)
    # max_size=(800,800) nghĩa là scale ảnh sao cho width hoặc height tối đa <=800px
    compress_image(local_input, local_output, quality=75, max_size=(800, 800))

    # 5) Upload hậu quả lên prefix processed/
    dest_key = latest_key.replace(PREFIX_UPLOAD, PREFIX_PROCESSED, 1)
    # Nếu bạn muốn giữ nguyên tên thư mục con, chỉ thay 'uploads/' thành 'processed/'
    # VD: uploads/2025-06-05/pic.jpg -> processed/2025-06-05/pic.jpg
    upload_to_s3(local_output, dest_key)

    print("Done processing latest image.")

if __name__ == "__main__":
    main()
