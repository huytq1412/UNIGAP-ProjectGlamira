import os
import logging
import json
import fastavro
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

avro_path = os.environ.get('AVRO_PATH')
if not avro_path:
    raise ValueError("LỖI: Biến AVRO_PATH chưa được khai báo trong file .env")

avro_foldername = os.path.abspath(os.path.expanduser(avro_path))

# ĐẢM BẢO THƯ MỤC TỒN TẠI: Nếu chưa có thì Python tự động tạo folder này
os.makedirs(avro_foldername, exist_ok=True)

# Thư mục lưu checkpoint load dữ liệu lên GCS
checkpoint_path = os.path.join(avro_foldername, 'checkpoints')
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

# Định nghĩa avro schemas
SCHEMAS = {
    "raw_data": {
        "type": "record",
        "name": "RawDataRecord",
        "fields": [
            {"name": "mongo_id", "type": ["null", "string"], "default": None},
            {"name": "api_version", "type": ["null", "string"], "default": None},
            {"name": "ip", "type": ["null", "string"], "default": None},
            {"name": "device_id", "type": ["null", "string"], "default": None},
            {"name": "user_id_db", "type": ["null", "string"], "default": None},
            {"name": "email_address", "type": ["null", "string"], "default": None},
            {"name": "user_agent", "type": ["null", "string"], "default": None},
            {"name": "resolution", "type": ["null", "string"], "default": None},
            {"name": "local_time", "type": ["null", "string"], "default": None},
            {"name": "time_stamp", "type": ["null", "long"], "default": None},
            {"name": "collection", "type": ["null", "string"], "default": None},
            {"name": "current_url", "type": ["null", "string"], "default": None},
            {"name": "referrer_url", "type": ["null", "string"], "default": None},
            {"name": "key_search", "type": ["null", "string"], "default": None},
            {"name": "utm_source", "type": ["null", "string"], "default": None},
            {"name": "utm_medium", "type": ["null", "string"], "default": None},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "viewing_product_id", "type": ["null", "string"], "default": None},
            {"name": "cat_id", "type": ["null", "string"], "default": None},
            {"name": "collect_id", "type": ["null", "string"], "default": None},
            {"name": "category_id", "type": ["null", "string"], "default": None},
            {"name": "store_id", "type": ["null", "string"], "default": None},
            {"name": "order_id", "type": ["null", "string"], "default": None},
            {"name": "price", "type": ["null", "string"], "default": None},
            {"name": "price_amount", "type": ["null", "string"], "default": None},
            {"name": "currency", "type": ["null", "string"], "default": None},
            {"name": "is_paypal", "type": ["null", "string"], "default": None},
            {"name": "show_recommendation", "type": ["null", "string"], "default": None},
            {"name": "recommendation", "type": ["null", "string"], "default": None},
            {"name": "recommendation_product_id", "type": ["null", "string"], "default": None},
            {"name": "recommendation_product_position", "type": ["null", "string"], "default": None},
            {
                "name": "option",
                "type": ["null", {
                    "type": "record",
                    "name": "RootOption",
                    "fields": [
                        {"name": "option_alloy", "type": ["null", "string"], "default": None},
                        {"name": "option_diamond", "type": ["null", "string"], "default": None},
                        {"name": "option_stone", "type": ["null", "string"], "default": None},
                        {"name": "option_quality", "type": ["null", "string"], "default": None},
                        {"name": "option_pearlcolor", "type": ["null", "string"], "default": None},
                        {"name": "option_finish", "type": ["null", "string"], "default": None},
                        {"name": "option_shapediamond", "type": ["null", "string"], "default": None},
                        {"name": "option_price", "type": ["null", "string"], "default": None},
                        {"name": "option_price_amount", "type": ["null", "string"], "default": None},
                        {"name": "option_Kollektion", "type": ["null", "string"], "default": None},
                        {"name": "option_kollektion_id", "type": ["null", "string"], "default": None},
                        {"name": "option_option_id", "type": ["null", "string"], "default": None},
                        {"name": "option_option_label", "type": ["null", "string"], "default": None},
                        {"name": "option_value_id", "type": ["null", "string"], "default": None},
                        {"name": "option_value_label", "type": ["null", "string"], "default": None},
                        {"name": "option_quality_label", "type": ["null", "string"], "default": None}
                    ]
                }], "default": None
            },
            {
                "name": "cart_products",
                "type": ["null", {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "CartProduct",
                        "fields": [
                            {"name": "product_id", "type": ["null", "string"], "default": None},
                            {"name": "price", "type": ["null", "string"], "default": None},
                            {"name": "amount", "type": ["null", "string"], "default": None},
                            {"name": "currency", "type": ["null", "string"], "default": None},
                            {
                                "name": "option",
                                "type": ["null", {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "CartProductOption",
                                        "fields": [
                                            {"name": "option_id", "type": ["null", "string"], "default": None},
                                            {"name": "option_label", "type": ["null", "string"], "default": None},
                                            {"name": "value_id", "type": ["null", "string"], "default": None},
                                            {"name": "value_label", "type": ["null", "string"], "default": None}
                                        ]
                                    }
                                }], "default": None
                            }
                        ]
                    }
                }], "default": None
            }
        ]
    },
    "product_names": {
        "type": "record",
        "name": "ProductNamesRecord",
        "fields": [
            {"name": "mongo_id", "type": ["null", "string"], "default": None},
            {"name": "product_id", "type": ["null", "string"], "default": None},
            {"name": "product_name", "type": ["null", "string"], "default": None},
            {"name": "url", "type": ["null", "string"], "default": None}
        ]
    },
    "ip_locations": {
        "type": "record",
        "name": "IpLocationsRecord",
        "fields": [
            {"name": "mongo_id", "type": ["null", "string"], "default": None},
            {"name": "ip", "type": ["null", "string"], "default": None},
            {"name": "country_short", "type": ["null", "string"], "default": None},
            {"name": "country_long", "type": ["null", "string"], "default": None},
            {"name": "region", "type": ["null", "string"], "default": None},
            {"name": "city", "type": ["null", "string"], "default": None}
        ]
    }
}

# DANH SÁCH CÁC CỘT HỢP LỆ TRÊN BIGQUERY (PHỄU LỌC)
OPTION_KEYS = [
    'option_alloy', 'option_diamond', 'option_stone', 'option_quality',
    'option_pearlcolor', 'option_finish', 'option_shapediamond',
    'option_price', 'option_price_amount', 'option_Kollektion',
    'option_kollektion_id', 'option_option_id', 'option_option_label',
    'option_value_id', 'option_value_label', 'option_quality_label'
]

def clean_root_option(root_option_data):
    # Giỏ tạm để chứa các key sau khi đã đắp tiền tố 'option_'
    result = {}

    # TRƯỜNG HỢP 1: Dữ liệu MongoDB là một Dictionary
    if isinstance(root_option_data, dict):
        for key, value in root_option_data.items():
            if value is not None:
                # Đắp 'option_' vào trước key gốc của phần tử (VD: 'option_label' -> 'option_option_label')
                new_key = f"option_{key}"
                result[new_key] = str(value)

    # TRƯỜNG HỢP 2: Dữ liệu MongoDB là một List
    elif isinstance(root_option_data, list):
        for item in root_option_data:
            # Kiểm tra mỗi phần tử trong mảng phải là một Dictionary
            if isinstance(item, dict):
                for key, value in item.items():
                    if value is not None:
                        # Đắp 'option_' vào trước key gốc của phần tử (VD: 'option_label' -> 'option_option_label')
                        new_key = f"option_{key}"
                        result[new_key] = str(value)

    # Lọc qua danh sách, chỉ giữ lại những giá trị được khai báo trong OPTION_KEYS
    final_result = {}
    for key, value in result.items():
        if key in OPTION_KEYS:
            final_result[key] = value

    return final_result

def standardlized_for_avro(doc, collection_name, is_root_option=True):
    # Hàm chuẩn hóa các cột cho file avro
    # Đổi _id thành mongo_id cho các bảng.
    # Ép tất cả các trường thành String (trừ time_stamp là Int) để khớp với Schema.

    if not isinstance(doc, dict):
        return doc

    cleaned = {}
    for key, value in doc.items():
        if value is None:
            cleaned[key] = None
            continue

        # Tự động map _id của MongoDB sang mongo_id để tránh lỗi BigQuery
        if key == '_id':
            cleaned['mongo_id'] = str(value)
            continue

        # Đảm bảo time_stamp là dạng số nguyên
        if collection_name == 'raw_data' and key == 'time_stamp':
            try:
                cleaned[key] = int(value)
            except (ValueError, TypeError):
                cleaned[key] = None
            continue

        if collection_name == 'raw_data' and key == 'option' and is_root_option:
            cleaned[key] = clean_root_option(value)
            continue

        # Xử lý lồng nhau (Nested data)
        if isinstance(value, dict):
            cleaned[key] = standardlized_for_avro(value, collection_name, is_root_option=False)
        elif isinstance(value, list):
            cleaned[key] = [standardlized_for_avro(item, collection_name, is_root_option=False) for item in value]
        else:
            # Tất cả các giá trị còn lại ép thành String theo đúng Schema BigQuery
            if not is_root_option and key == 'option' and value == '':
                cleaned[key] = None
            else:
                cleaned[key] = str(value)

    return cleaned

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

def convert_and_upload(collection, data_list, part_number, bucket):
    # Hàm phụ trách biến mảng thành Parquet và đẩy lên GCP

    # Chuyển đổi sang Avro
    filename = f"{collection}_part_{part_number}.avro"

    local_filename = os.path.join(avro_foldername, filename)

    # Xử lý chuẩn hóa dữ liệu để khớp với Avro Schema
    processed_data = [standardlized_for_avro(doc, collection) for doc in data_list]

    # Lấy và parse Schema
    schema = SCHEMAS.get(collection)
    parsed_schema = fastavro.parse_schema(schema)

    # Ghi file Avro
    with open(local_filename, 'wb') as output:
        fastavro.writer(output, parsed_schema, processed_data)

    # Upload lên GCS
    gcs_dest = f"raw_layer/{collection}/{filename}"
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