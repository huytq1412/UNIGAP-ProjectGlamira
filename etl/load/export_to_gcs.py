import os
import logging
import json
import pandas as pd
from google.cloud import storage
from config.get_mongo_connection import get_database, close_connection
from src.get_data_from_env import get_filename
from dotenv import load_dotenv
import time
from bson.objectid import ObjectId

COLLECTIONS = ["product_names", "raw_data", "ip_locations"]
CHUNK_SIZE = 250000 # Lượng data đạt ngưỡng để đẩy lên GCP
BATCH_SIZE = 5000 # Lượng data lấy từ MongoDB mỗi lần

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

parquet_path = os.environ.get('PARQUET_PATH')

if not parquet_path:
    raise ValueError("LỖI: Biến PARQUET_PATH chưa được khai báo trong file .env")

parquet_foldername = os.path.abspath(os.path.expanduser(parquet_path))

# ĐẢM BẢO THƯ MỤC TỒN TẠI: Nếu chưa có thì Python tự động tạo folder này
os.makedirs(parquet_foldername, exist_ok=True)

# Thư mục lưu checkpoint load dữ liệu lên GCS
checkpoint_path = os.path.join(parquet_foldername, 'checkpoints')
os.makedirs(checkpoint_path, exist_ok=True)

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
    logging.info("Không tìm thấy File JSON. Sẽ sử dụng quyền mặc định của VM (GCP Mode).")

def get_checkpoint(collection):
    # Đọc file checkpoint xem lần trước chạy đến đâu
    checkpoint_file = os.path.join(checkpoint_path, f"{collection}_checkpoint.json")
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            data = json.load(f)
            return data.get('last_id'), data.get('part_number', 0)
    return None, 0

def save_checkpoint(collection, last_id, part_number):
    # Lưu lại ID cuối cùng và số thứ tự part hiện tại
    checkpoint_file = os.path.join(checkpoint_path, f"{collection}_checkpoint.json")
    with open(checkpoint_file, 'w') as f:
        json.dump({'last_id': str(last_id), 'part_number': part_number}, f)

def clear_checkpoint(collection):
    # Xóa file checkpoint khi đã xuất xong 100% dữ liệu
    checkpoint_file = os.path.join(checkpoint_path, f"{collection}_checkpoint.json")
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

def standardlized_for_parquet(df):
    # Xử lý riêng cho cột cat_id
    if 'cat_id' in df.columns:
        # Biến ô trống dữ liệu thành chuỗi rỗng thay vì chữ 'nan'
        df['cat_id'] = df['cat_id'].fillna('').astype(str)

    for col in df.columns:
        # Bỏ qua các cột đã là kiểu số/ngày tháng rõ ràng, chỉ xử lý cột 'object' (kiểu thập cẩm)
        if df[col].dtype == 'object':

            # TẠO MẶT NẠ LỌC: Chỉ tìm những ô KHÔNG BỊ TRỐNG
            valid_mask = df[col].notna()

            # Nếu cột trống dữ liệu, bỏ qua luôn
            if not valid_mask.any():
                continue

            # Thay vì quét apply toàn bộ cột, ta chỉ lấy mẫu 100 dòng đầu có chứa data để đoán định dạng
            sample_data = df.loc[valid_mask, col].head(100)
            has_complex_type = sample_data.apply(lambda x: isinstance(x, (list, dict))).any()

            # Nếu cột chứa dữ liệu phức tạp (LIST/DICT)
            if has_complex_type:
                processed_values = []

                # Lấy ra đúng những ô CÓ DỮ LIỆU (bỏ qua ô trống) để mang đi xử lý
                data_to_process = df.loc[valid_mask, col]

                # Lặp qua từng ô để xử lý kiểu dữ liệu
                for x in data_to_process:
                    if isinstance(x, (list, dict)):
                        # Nếu là mảng -> ép json biến thành string
                        json_str = json.dumps(x, ensure_ascii=False)
                        processed_values.append(json_str)

                    else:
                        # Nếu chỉ là số hoặc chữ -> Ép thành string
                        obj_str = str(x)
                        processed_values.append(obj_str)

                # Lấy toàn bộ kết quả đã xử lý đè ngược lại vào bảng DataFrame
                df.loc[valid_mask, col] = processed_values

            # Nếu cột chứa dữ liệu đơn giản
            else:
                df.loc[valid_mask, col] = df.loc[valid_mask, col].astype(str)

    return df

def convert_and_upload(collection, data_list, part_number, bucket):
    # Hàm phụ trách biến mảng thành Parquet và đẩy lên GCP

    # Chuyển đổi sang Parquet
    filename = f"{collection}_part_{part_number}.parquet"

    local_filename = os.path.join(parquet_foldername, filename)

    df = pd.DataFrame(data_list)

    df = standardlized_for_parquet(df)

    df.to_parquet(local_filename, engine='pyarrow', index=False)

    # Upload lên GCS
    gcs_dest = f"raw/{collection}/{filename}"
    blob = bucket.blob(gcs_dest)

    logging.info(f"Đang upload Part {part_number} (Kích thước: {len(data_list)} dòng)...")

    blob.upload_from_filename(local_filename)

    # Xóa file tạm
    os.remove(local_filename)

def export_to_gcs():
    start_time = time.time()

    # 1. Connect to MongoDB (or VM)
    try:
        db = get_database()

        # Kết nối với GCP xác thực bằng file json gcp_key
        client = storage.Client()

        # Chọn tới bucket cần lưu trữ
        bucket = client.bucket(bucket_name)
    except Exception as e:
        logging.error(f"Lỗi khởi tạo kết nối: {e}")
        return

    logging.info("BẮT ĐẦU PIPELINE XUẤT DỮ LIỆU LÊN GCS...")

    # 2. Extract data in batches
    # Lặp qua từng bảng
    for collection in COLLECTIONS:
        logging.info(f"\n========== ĐANG XỬ LÝ: {collection.upper()} ==========")
        src_collection = db[collection]

        # ĐỌC CHECKPOINT VÀ GẮN VÀO CÂU QUERY
        last_id, last_part = get_checkpoint(collection)
        query = {}

        if last_id:
            # Nếu có checkpoint, chỉ lấy những dòng có ID LỚN HƠN ID đã lưu
            query['_id'] = {'$gt': ObjectId(last_id)}
            part_number = last_part + 1
            logging.info(f"KHÔI PHỤC: Chạy tiếp từ Part {part_number} (Sau _id: {last_id})")
        else:
            part_number = 1 # Chia phần với những collection có quá nhiều dữ liệu
            logging.info("CHẠY MỚI: Bắt đầu từ Part 1")

        # Tạo cursor, mỗi lần kéo 5000 dòng
        cursor = src_collection.find(query).sort('_id', 1).batch_size(BATCH_SIZE)

        batch_data = []
        processed = 0 # Tổng bản ghi dữ liệu đã xử lý

        try:
            for doc in cursor:
                batch_data.append(doc)
                processed += 1

                # Xử lý đẩy dữ liệu vào GCP khi đủ ngưỡng
                if len(batch_data) >= CHUNK_SIZE:
                    # Lưu lại ID cuối cùng làm checkpoint
                    current_last_id = batch_data[-1]['_id']

                    # 3. Convert to appropriate format (CSV/JSONL/PARQUET/ARVO/ORC)
                    # 4. Upload to GCS (all data in VM or in MongoDB)
                    convert_and_upload(collection, batch_data, part_number, bucket)

                    # Ghi checkpoint vào file
                    save_checkpoint(collection, current_last_id, part_number)
                    logging.info(f"Đã đánh dấu Checkpoint Part {part_number}")

                    logging.info(f"collection: {collection} part: {part_number}")

                    batch_data.clear()
                    part_number += 1

            # Xử lý nốt số dữ liệu lẻ còn dư ở batch cuối cùng
            if len(batch_data) > 0:
                # Lưu lại ID cuối cùng làm checkpoint
                current_last_id = batch_data[-1]['_id']

                # 3. Convert to appropriate format (CSV/JSONL/PARQUET/ARVO/ORC)
                # 4. Upload to GCS (all data in VM or in MongoDB)
                convert_and_upload(collection, batch_data, part_number, bucket)

                # Ghi checkpoint vào file
                save_checkpoint(collection, current_last_id, part_number)

                batch_data.clear()

                logging.info( f"Xử lý batch còn dư: collection: {collection} part: {part_number}")

            # Xóa checkpoint khi đã xong 100%
            # clear_checkpoint(collection)

            logging.info(f"Hoàn tất {collection}! Tổng cộng đã xuất: {processed} dòng.")
        except Exception as e:
            logging.error(f"Lỗi khi trích xuất bảng {collection}: {e}")

    close_connection()
    client.close()

    # 5. Log operations
    logging.info(f"Tổng thời gian: {time.time() - start_time:.2f}s ({(time.time() - start_time) / 60:.2f} phút)")

if __name__ == '__main__':
    export_to_gcs()