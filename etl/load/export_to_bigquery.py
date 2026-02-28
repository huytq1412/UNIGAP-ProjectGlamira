import os
import logging
from google.cloud import bigquery
from src.get_data_from_env import get_filename
from dotenv import load_dotenv
import time

COLLECTIONS = ["product_names", "raw_data", "ip_locations"]
DATASET = "raw_layer"

# Cấu hình Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Lấy thư mục file hiện tại
current_dir = os.path.dirname(__file__)

# Lấy thư mục gốc của project
project_dir = os.path.abspath(os.path.join(current_dir, '../..'))

# Lấy thư mục file .env
env_path = os.path.join(project_dir, '.env')

# Load file, nếu không thấy file sẽ báo
if load_dotenv(dotenv_path=env_path):
    logging.info("Đã load thành công cấu hình từ file .env")
else:
    logging.warning("Không tìm thấy file .env, đang sử dụng biến môi trường có sẵn của hệ thống.")

bucket_name = os.environ.get('BUCKET_NAME')

if not bucket_name:
    raise ValueError("LỖI: Biến BUCKET_NAME chưa được khai báo trong file .env")

gcp_key_path = os.environ.get('GCP_KEY_FILE_PATH')

# LOGIC TỰ ĐỘNG NHẬN DIỆN MÔI TRƯỜNG
if gcp_key_path:
    # Trường hợp 1: Chạy ở Local (có khai báo đường dẫn JSON trong .env)
    gcp_key_filename = get_filename(gcp_key_path, 'GCP_KEY_FILE_PATH')

    # Cấp thẻ quyền cho GCP
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gcp_key_filename

    logging.info("Đang dùng File JSON để xác thực (Local Mode).")
else:
    # Trường hợp 2: Chạy trên VM (Lệnh storage.Client() ở dưới sẽ tự động dùng quyền của VM)
    logging.info("Không tìm thấy File JSON. Sẽ sử dụng quyền mặc định của VM.")


def export_to_bigquery():
    start_time = time.time()

    # 1. Khởi tạo đối tượng BigQuery Client
    try:
        # Kết nối với BigQuery xác thực bằng file json gcp_key
        client = bigquery.Client()

        logging.info(f"Đã kết nối BigQuery! Project ID đang dùng: {client.project}")
    except Exception as e:
        logging.error(f"Lỗi khởi tạo kết nối: {e}")
        return

    # 2. Vòng lặp nạp dữ liệu cho từng bảng
    for collection in COLLECTIONS:
        logging.info(f"\n========== ĐANG LOAD DỮ LIỆU BẢNG: {collection.upper()} ==========")

        # Đường dẫn Nguồn: Trỏ đến toàn bộ file parquet của bảng tương ứng trên GCS
        gcs_uri = f"gs://{bucket_name}/raw/{collection}/*.parquet"

        # Đường dẫn Đích
        table = f"{client.project}.{DATASET}.{collection}"

        # Tạo LoadJob config
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Xóa bảng cũ, đè dữ liệu mới
            autodetect=True
        )

        try:
            # Kích hoạt tiến trình LoadJob
            load_job = client.load_table_from_uri(
                gcs_uri,
                table,
                job_config=job_config
            )

            load_job.result()

            # Kiểm tra lại kết quả trên BigQuery
            destination_table = client.get_table(table)
            logging.info(f"THÀNH CÔNG! Đã nạp trọn vẹn {destination_table.num_rows} dòng vào bảng {collection}.")
        except Exception as e:
            logging.error(f"Lỗi nghiêm trọng khi nạp bảng {collection}: {e}")

    logging.info(f"Tổng thời gian nạp BigQuery: {time.time() - start_time:.2f}s ({(time.time() - start_time) / 60:.2f} phút)")

if __name__ == "__main__":
    export_to_bigquery()