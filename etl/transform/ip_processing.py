import IP2Location
import time
import os
from config.get_mongo_connection import get_database, close_connection
from src.get_data_from_env import get_filename
from dotenv import load_dotenv

SOURCE_COLLECTION = 'raw_data'  # Collection chứa dữ liệu gốc
TARGET_COLLECTION = 'ip_locations'  # Collection mới để chứa kết quả
IP_FIELD_NAME = 'ip'  # Tên trường chứa IP

# Lấy thư mục file hiện tại
current_dir = os.path.dirname(__file__)

# Lấy thư mục gốc của project
project_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Lấy thư mục file .env
env_path = os.path.join(project_dir, '.env')

load_dotenv(dotenv_path=env_path)

ip_data_path = os.environ.get('IP_DATA_PATH')

ip_filename = get_filename(ip_data_path, 'IP_DATA_PATH')

# --- 1. DATA QUALITY: NORMALIZE & VALIDATE ---
def normalize_data(ip_str, record):
    """
    Chuyển đổi object của IP2Location thành dict chuẩn.
    Xử lý các giá trị None, whitespace.
    """
    # IP2Location thường trả về '-' hoặc 'This parameter is unavailable...' nếu thiếu data
    # Ta sẽ giữ nguyên raw text ở bước này, xử lý logic ở bước validate
    return {
        'ip': str(ip_str).strip(),
        'country_short': str(record.country_short if record.country_short else '').strip(),
        'country_long': str(record.country_long if record.country_long else '').strip(),
        'region': str(record.region if record.region else '').strip(),
        'city': str(record.city if record.city else '').strip()
    }

def validate_data(item):
    """
    Kiểm tra xem thông tin vị trí có hợp lệ không.
    Trả về: (Bool, Reason)
    """
    if not item['ip']:
        return False, 'missing_ip'

    # Định nghĩa các giá trị được coi là "Thiếu"
    missing_markers = ['-', '', 'n/a', 'N/A']

    # Logic Strict: Phải có đủ Country Short, Region VÀ City mới tính là Valid
    if (item['country_short'] in missing_markers or item['country_long'] in missing_markers or
        item['region'] in missing_markers or item['city'] in missing_markers):
        return False, 'missing_location_info'

    if item['country_long'].upper() == 'INVALID IP ADDRESS':
        return False, 'invalid_ip'

    return True, 'valid'

def process_ip_locations():
    # Kiểm tra file BIN có tồn tại không
    if not os.path.exists(ip_filename):
        print(f"Lỗi: Không tìm thấy file '{ip_filename}'.")
        return

    # 1. Connect to MongoDB
    db = get_database()
    src_collection = db[SOURCE_COLLECTION]
    tgt_collection = db[TARGET_COLLECTION]

    print(f"Đang kiểm tra và tạo Index cho trường {IP_FIELD_NAME}...")
    src_collection.create_index([(IP_FIELD_NAME, 1)])

    # Xóa dữ liệu cũ trong bảng kết quả (nếu có) để chạy lại cho sạch
    tgt_collection.delete_many({})

    # 2. Read unique IPs from main collection
    # print(f"Đang quét danh sách IP duy nhất từ '{SOURCE_COLLECTION}'...")
    # ip_unique = src_collection.distinct(IP_FIELD_NAME)
    # ip_count = len(ip_unique)
    # print(f"=> Tìm thấy {ip_count} IP duy nhất.")

    # Bước 2a: Đếm tổng số IP duy nhất trước (để làm stats)
    count_pipeline = [
        {"$group": {"_id": f"${IP_FIELD_NAME}"}},
        {"$count": "total_ips"}
    ]
    # allowDiskUse=True: Cho phép MongoDB dùng ổ cứng để sort/group nếu RAM server không đủ
    count_result = list(src_collection.aggregate(count_pipeline, allowDiskUse=True))
    ip_count = count_result[0]['total_ips'] if count_result else 0

    print(f"=> Tìm thấy {ip_count} IP duy nhất.")

    # Bước 2b: Tạo cursor để duyệt dữ liệu (Stream)
    data_pipeline = [
        {"$group": {"_id": f"${IP_FIELD_NAME}"}}
    ]
    ip_unique = src_collection.aggregate(data_pipeline, allowDiskUse=True)

    # 3. Use ip2location to get location data
    # Load dữ liệu IP từ file BIN
    try:
        ip_data = IP2Location.IP2Location(ip_filename)
    except Exception as e:
        print(f"Lỗi đọc file BIN: {e}")

    stats = {
        'total': ip_count,
        'processed': 0,
        'success': 0,  # Đã lưu vào DB
        'dq_invalid_ip': 0,  # IP sai format
        'dq_missing_location_info': 0,  # Thiếu Country/Region/City (Gom chung)
        'dq_missing_ip': 0,  # Input rỗng
        'err_exception': 0
    }

    # 4. Store results in new collection Or CSV file
    # Tạo một danh sách rỗng để chứa các data lấy được từ địa chỉ ip
    ip_data_list = []
    inserted_count = 0
    start_time = time.time()

    print(f"\n{'=' * 40}")
    print(f"BẮT ĐẦU XỬ LÝ IP")
    # In header log mới
    print(f"{'Processed':<10} | {'Pending':<10} | {'Success':<10} | {'Missing IP':<10} | {'Missing Loc':<10} | {'Invalid':<10}")
    print("-" * 80)
    print(f"{0:<10} | {stats['total']:<10} | {stats['success']:<10} | {stats['dq_missing_ip']:<10} | {stats['dq_missing_location_info']:<10} | {stats['dq_invalid_ip']:<10}")

    for ip in ip_unique:
        stats['processed'] += 1

        # if not ip:
        #     continue

        # SỬA Ở ĐÂY: Lấy chính xác chuỗi IP ra khỏi dictionary của MongoDB
        ip_str = ip.get('_id')

        if not ip_str:
            stats['dq_missing_ip'] += 1
            continue

        try:
            document = ip_data.get_all(ip_str)

            normalized_item = normalize_data(ip_str, document)
            # Kiểm tra
            is_valid, reason = validate_data(normalized_item)

            if is_valid:
                stats['success'] += 1
                ip_data_list.append(normalized_item)
            else:
                dq_key = f"dq_{reason}"
                if dq_key in stats:
                    stats[dq_key] += 1
        except Exception as e:
            print(f"{e}")
            continue

        # Khi nào đủ 10000 document thì insert một thể
        if len(ip_data_list) >= 10000:
            tgt_collection.insert_many(ip_data_list)
            inserted_count += len(ip_data_list)
            pending = stats['total'] - stats['processed']
            # Reset thùng chứa
            ip_data_list = []
            print(f"{stats['processed']:<10} | {pending:<10} | {stats['success']:<10} | {stats['dq_missing_ip']:<10} | {stats['dq_missing_location_info']:<10} | {stats['dq_invalid_ip']:<10}")

    # Ghi nốt số dữ liệu còn dư
    if ip_data_list:
        tgt_collection.insert_many(ip_data_list)
        inserted_count += len(ip_data_list)
        pending = 0
        print(f"{stats['processed']:<10} | {pending:<10} | {stats['success']:<10} | {stats['dq_missing_ip']:<10} | {stats['dq_missing_location_info']:<10} | {stats['dq_invalid_ip']:<10}")

    close_connection()
    duration = time.time() - start_time

    print("\n" + "=" * 40)
    print(f"HOÀN TẤT IP PROCESSING")
    print("-" * 50)
    print(f"Tổng thời gian          : {duration:.2f}s")
    print(f"Tổng Input              : {stats['total']}")
    print("-" * 30)
    print(f"Success Data (Inserted) : {stats['success']}")
    print("-" * 30)
    print(f"Invalid (Skipped):")
    print(f" - Missing IP           : {stats['dq_missing_ip']}")
    print(f" - Missing Location     : {stats['dq_missing_location_info']} ")
    print(f" - Invalid IP           : {stats['dq_invalid_ip']}")
    print("-" * 50)
    print(f"MongoDB: Saved to '{TARGET_COLLECTION}'")

if __name__ == "__main__":
    process_ip_locations()