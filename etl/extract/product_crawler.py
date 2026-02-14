import csv
import time
import os
import concurrent.futures
from bs4 import BeautifulSoup
import random
from src.network import get_session
from config.get_mongo_connection import get_database, close_connection
from src.get_data_from_env import get_filename
from dotenv import load_dotenv
from src.get_headers import get_random_headers

SOURCE_COLLECTION = 'raw_data'  # Collection chứa dữ liệu gốc
TARGET_COLLECTION = 'product_names'  # Collection mới để chứa kết quả
BATCH_SIZE = 100
WORKERS = 10

GROUP1 = [
    'view_product_detail',
    'select_product_option',
    'select_product_option_quality',
    'add_to_cart_action',
    'product_detail_recommendation_visible',
    'product_detail_recommendation_noticed'
]

GROUP2 = [
    'product_view_all_recommend_clicked'
]

BROWSER_LIST = [
    "chrome120",
    "chrome119",
    "edge101",
    "safari15_3"
]

# Lấy thư mục file hiện tại
current_dir = os.path.dirname(__file__)

# Lấy thư mục gốc của project
project_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Lấy thư mục file .env
env_path = os.path.join(project_dir, '.env')

load_dotenv(dotenv_path=env_path)

product_name_path = os.environ.get('PRODUCT_NAME_PATH')

OUTPUT_CSV = get_filename(product_name_path, 'PRODUCT_NAME_PATH')


def get_total():
    db = get_database()
    src_collection = db[SOURCE_COLLECTION]
    products_set = set()

    condition_group1 = {"collection": {"$in": GROUP1}}
    field_group1 = {"product_id": 1, "viewing_product_id": 1}
    doc_group1 = src_collection.find(condition_group1, field_group1)

    for doc in doc_group1:
        product_id = doc.get('product_id') or doc.get('viewing_product_id')
        if product_id:
            products_set.add(product_id)

    condition_group2 = {"collection": {"$in": GROUP2}}
    field_group2 = {"viewing_product_id": 1}
    doc_group2 = src_collection.find(condition_group2, field_group2)

    for doc in doc_group2:
        product_id = doc.get('viewing_product_id')
        if product_id:
            products_set.add(product_id)

    return len(products_set)

def get_total():
    db = get_database()
    src_collection = db[SOURCE_COLLECTION]

    # 1. Chỉ lấy những field thực sự cần thiết (Projection)
    # _id: 0 nghĩa là không lấy _id (tiết kiệm băng thông)
    field_group = {
        "collection": 1,
        "product_id": 1,
        "viewing_product_id": 1,
        "_id": 0
    }

    # 2. Lọc ngay từ đầu
    condition_group = {"collection": {"$in": GROUP1 + GROUP2}}

    # Lấy con trỏ (cursor) về, chưa tải data ngay
    cursor = src_collection.find(condition_group, field_group)

    products_set = set()

    # 3. Xử lý logic bằng Python (Dễ đọc, dễ sửa)
    for doc in cursor:
        productid = None
        col_type = doc.get("collection")

        # Logic if/else rõ ràng của Python
        if col_type in GROUP1:
            # Ưu tiên product_id, nếu không có thì lấy viewing
            productid = doc.get("product_id") or doc.get("viewing_product_id")
        else:
            # Group 2
            productid = doc.get("viewing_product_id")

        if productid:
            products_set.add(productid)

    return len(products_set)

def get_product():
    db = get_database()
    src_collection = db[SOURCE_COLLECTION]
    products_set = set()

    condition_group1 = {"collection": {"$in": GROUP1}}
    field_group1 = {"product_id": 1, "viewing_product_id": 1, "current_url": 1}
    doc_group1 = src_collection.find(condition_group1, field_group1)

    for doc in doc_group1:
        product_id = doc.get('product_id') or doc.get('viewing_product_id')
        url = doc.get('current_url')

        # print(2)
        if product_id and url and isinstance(url, str) and product_id not in products_set:
            products_set.add(product_id)
            yield {'product_id': product_id, 'url': url}

    condition_group2 = {"collection": {"$in": GROUP2}}
    field_group2 = {"viewing_product_id": 1, "referrer_url": 1}
    doc_group2 = src_collection.find(condition_group2, field_group2)

    for doc in doc_group2:
        product_id = doc.get('viewing_product_id')
        url = doc.get('referrer_url')

        if product_id and url and isinstance(url, str) and product_id not in products_set:
            products_set.add(product_id)
            yield {'product_id': product_id, 'url': url}


def name_scrapping(item):
    # Giúp request không bị gửi dồn dập cùng 1 lúc -> Server đỡ nghi ngờ
    time.sleep(random.uniform(0.5, 1.5))
    # time.sleep(random.uniform(2, 5))
    product_id = item['product_id']
    url = item['url']

    # Lấy session (đã cấu hình tự động Retry cho 5xx)
    session = get_session()

    # session = requests.Session()  # Dùng session của curl_cffi
    # # Chọn ngẫu nhiên 1 kiểu trình duyệt
    # browser_type = random.choice(BROWSER_LIST)

    # Cấu hình số lần thử lại
    max_retry = 3

    for attempt in range(max_retry):
        try:
            # Gọi request (Nếu lỗi 5xx hoặc mất mạng, Session Adapter sẽ tự retry 3 lần ở dòng này rồi mới trả kết quả)
            headers = get_random_headers()
            res = session.get(url, headers=headers, timeout=10)

            # res = session.get(url, timeout=10)

            # res = session.get(
            #     url,
            #     timeout=10,
            #     impersonate=browser_type
            # )
            # --- TRƯỜNG HỢP 1: THÀNH CÔNG (200) ---
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "html5lib")
                product_name = "Unknown name"

                # (Logic lấy tên giữ nguyên...)
                h1_tag = soup.select_one('h1.page-title span')
                if h1_tag:
                    text = h1_tag.get_text(strip=True)
                    if text: product_name = text

                if product_name == "Unknown name":
                    meta_tag = soup.find('meta', property='og:title')
                    if meta_tag:
                        content = meta_tag.get('content')
                        if content: product_name = content

                return {'product_id': product_id, 'product_name': product_name, 'url': url,
                        'status': 'success', 'http_code': 200, 'error_msg': ''}

            # --- TRƯỜNG HỢP 2: LỖI 404, 403 -> KHÔNG RETRY
            elif res.status_code == 404:
                return {'product_id': product_id, 'url': url, 'status': 'err_404',
                        'http_code': 404, 'error_msg': 'Page Not Found'}

            elif res.status_code == 403:
                print('err_403')
                print(product_id)
                time.sleep(60)
                return {'product_id': product_id, 'url': url, 'status': 'err_403',
                        'http_code': 403, 'error_msg': 'Forbidden'}

            # --- TRƯỜNG HỢP 3: LỖI 429 (RATE LIMIT) -> CẦN RETRY
            elif res.status_code == 429:
                print('err_429')
                print(f"429 Rate Limit - ID {product_id}. Sleeping 5s... (Attempt {attempt + 1})")
                time.sleep(random.uniform(30, 60))
                continue  # Quay lại đầu vòng lặp để thử lại

            # --- TRƯỜNG HỢP 4: CÁC LỖI KHÁC ---
            # Nếu chạy đến đây nghĩa là Adapter đã retry 5xx hết mức mà vẫn lỗi
            else:
                # Nếu chưa phải lần cuối thì sleep rồi thử lại (cho chắc ăn)
                if attempt < max_retry - 1:
                    time.sleep(2)
                    continue

                return {'product_id': product_id, 'url': url, 'status': 'err_5xx',
                        'http_code': res.status_code, 'error_msg': f'HTTP {res.status_code}'}

        except Exception as e:
            # Lỗi ngoại lệ
            if attempt < max_retry - 1:
                time.sleep(2)
                continue

            return {'product_id': product_id, 'url': url, 'status': 'err_exception',
                    'http_code': 0, 'error_msg': str(e)}

    # Nếu hết vòng lặp mà vẫn dính 429 hoặc lỗi khác
    return {'product_id': product_id, 'url': url, 'status': 'err_429',
            'http_code': 429, 'error_msg': 'Rate limit'}


# DATA QUALITY (CHUẨN HÓA & VALIDATE)
def normalize_data(raw_item):
    """Chuẩn hóa dữ liệu trước khi lưu"""
    return {
        'product_id': str(raw_item.get('product_id', '')).strip(),
        'product_name': str(raw_item.get('product_name', '')).strip(),
        'url': str(raw_item.get('url', '')).strip()
    }


def validate_data(item):
    """Kiểm tra dữ liệu sạch"""
    if not item['product_id']:
        return False, 'missing_id'
    # Tên sản phẩm phải có và không được là "Unknown name"
    if not item['product_name'] or item['product_name'].lower() == 'unknown name':
        return False, 'missing_name'
    if not item['url']:
        return False, 'missing_url'
    return True, 'valid'


# Hàm xử lý danh sách kết quả (Dùng chung cho cả batch và phần dư)
def process_results(results_list, csv_writer):
    insert_mongo_batch = []
    for result in results_list:
        # 1. Ghi log CSV (Ghi tất cả kết quả, kể cả lỗi)
        csv_writer.writerow(result)

        # 2. Cập nhật thống kê mạng
        stats['processed'] += 1
        status_key = result['status']
        if status_key == 'success':
            stats['success'] += 1
        elif status_key in stats:
            stats[status_key] += 1
        else:
            stats['err_other'] += 1

        # 3. Xử lý Data Quality & Chuẩn bị ghi Mongo
        if status_key == 'success':
            # Chuẩn hóa
            normalized_item = normalize_data(result)
            # Kiểm tra
            is_valid, reason = validate_data(normalized_item)

            if is_valid:
                stats['dq_valid'] += 1
                insert_mongo_batch.append(normalized_item)
            else:
                dq_key = f"dq_{reason}"
                if dq_key in stats:
                    stats[dq_key] += 1
    return insert_mongo_batch


if __name__ == "__main__":
    db = get_database()
    src_collection = db[SOURCE_COLLECTION]
    tgt_collection = db[TARGET_COLLECTION]

    # Xóa dữ liệu cũ trong bảng kết quả (nếu có) để chạy lại cho sạch
    tgt_collection.delete_many({})

    # --- BIẾN THỐNG KÊ CHI TIẾT ---
    stats = {
        'total': get_total(),  # Tổng số bản ghi
        'processed': 0,  # Số bản ghi đã xử lý
        'success': 0,  # Số bản ghi thành công
        'err_404': 0,  # Lỗi 404
        'err_403': 0,  # Lỗi 403
        'err_429': 0,  # Lỗi 429
        'err_5xx': 0,  # Lỗi 5xx
        'err_exception': 0,  # Lỗi Exception (Timeout, DNS...),
        'err_other': 0, # Lỗi khác
        # Data quality checks
        'dq_valid': 0,
        'dq_missing_id': 0,
        'dq_missing_name': 0,
        'dq_missing_url': 0
    }

    products_list = []
    inserted_count = 0
    start_time = time.time()
    batch_start_time = time.time()

    print("\n" + "=" * 40)
    print(f"BẮT ĐẦU CRAWLING")
    print(f"{'Chunk':<6} | {'Pending':<8} | {'OK':<6} | {'Valid':<6} | {'404':<5} | {'403':<5} | {'429':<5} | {'5xx':<5} | {'Net':<5} | {'Time (s)':<10}")
    print("-" * 85)
    print(f"{0:<6} | {stats['total']:<8} | {stats['success']:<6} | {stats['dq_valid']:<6} | {stats['err_404']:<5} | {stats['err_403']:<5} | {stats['err_429']:<5} | {stats['err_5xx']:<5} | {stats['err_exception']:<5} | 0s")

    with open(OUTPUT_CSV, mode='w', encoding='utf-8-sig', newline='') as f:
        field_names = ['product_id', 'product_name', 'url', 'status', 'http_code', 'error_msg']

        writer = csv.DictWriter(f, fieldnames=field_names)

        # Ghi header
        writer.writeheader()

        # Sử dụng concurrent.futures để có nhiều luồng thu thập crawl dữ liệu hơn
        # Khởi tạo bể chứa các luồng
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            for item in get_product():
                products_list.append(item)
                # print(len(products_list))

                if len(products_list) >= BATCH_SIZE:
                    results = list(executor.map(name_scrapping, products_list))

                    product_data_list = []
                    # print(4)
                    # Gọi hàm xử lý trung tâm
                    insert_mongo_batch = process_results(results, writer)

                    # Insert vào Mongo (Chỉ data sạch)
                    if insert_mongo_batch:
                        try:
                            tgt_collection.insert_many(insert_mongo_batch)
                        except Exception as e:
                            # Log nhẹ nếu có lỗi insert (thường là duplicate key nếu có index)
                            pass

                    # --- XỬ LÝ LOGGING ---
                    current_time = time.time()
                    batch_duration = current_time - batch_start_time
                    batch_start_time = current_time

                    chunk_num = stats['processed'] // BATCH_SIZE
                    pending = stats['total'] - stats['processed']

                    print(f"{chunk_num:<6} | {pending:<8} | {stats['success']:<6} | {stats['dq_valid']:<6} | {stats['err_404']:<5} | {stats['err_403']:<5} | {stats['err_429']:<5} | {stats['err_5xx']:<5} | {stats['err_exception']:<5} | {batch_duration:.2f}s")

                    products_list = []  # Reset batch đầu vào

            # Ghi nốt số dữ liệu còn dư
            if products_list:
                results = list(executor.map(name_scrapping, products_list))

                insert_mongo_batch = []

                # Gọi hàm xử lý trung tâm
                insert_mongo_batch = process_results(results, writer)

                # Insert vào Mongo (Chỉ data sạch)
                if insert_mongo_batch:
                    try:
                        tgt_collection.insert_many(insert_mongo_batch)
                    except Exception as e:
                        # Log nhẹ nếu có lỗi insert (thường là duplicate key nếu có index)
                        pass

                # XỬ LÝ LOGGING lô cuối
                current_time = time.time()
                batch_duration = current_time - batch_start_time
                chunk_num = (stats['processed'] // BATCH_SIZE) + 1
                pending = 0

                print(f"{chunk_num:<6} | {pending:<8} | {stats['success']:<6} | {stats['dq_valid']:<6} | {stats['err_404']:<5} | {stats['err_403']:<5} | {stats['err_429']:<5} | {stats['err_5xx']:<5} | {stats['err_exception']:<5} | {batch_duration:.2f}s")

    close_connection()
    total_duration = time.time() - start_time

    # --- FINAL REPORT ---
    print("\n" + "=" * 40)
    print(f"HOÀN TẤT CRAWLING!")
    print(f"BÁO CÁO TỔNG KẾT")
    print("-" * 50)
    print(f"Tổng thời gian  : {total_duration:.2f}s ({total_duration / 60:.2f} phút)")
    print(f"Tổng input      : {stats['total']}")
    print("-" * 30)
    print(f"SUCCESS (200)   : {stats['success']} ({stats['success'] / stats['total'] * 100:.1f}%)")
    print(f"Data Valid      : {stats['dq_valid']}")
    print(f"Data Invalid")
    print(f" - Missing ID   : {stats['dq_missing_id']}")
    print(f" - Missing Name : {stats['dq_missing_name']}")
    print(f" - Missing Url  : {stats['dq_missing_url']}")
    print("-" * 30)
    print(f"404 Not Found   : {stats['err_404']}")
    print(f"403 Forbidden   : {stats['err_403']}")
    print(f"429 RateLimit   : {stats['err_429']}")
    print(f"5xx Server Err  : {stats['err_5xx']}")
    print(f"Network Err     : {stats['err_exception']}")
    print(f"Other Errors    : {stats['err_other']}")
    print("-" * 50)
    print(f"MongoDB: Đã lưu vào collection: '{TARGET_COLLECTION}'")
    print(f"CSV: Đã lưu vào '{OUTPUT_CSV}'")
