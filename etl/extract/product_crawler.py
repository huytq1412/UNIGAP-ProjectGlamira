import csv
import time
import os
import concurrent.futures
from bs4 import BeautifulSoup
import random
from dotenv import load_dotenv
from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError
from config.get_mongo_connection import get_database, close_connection
from src.get_data_from_env import get_filename
from src.checkpoint_manager import save_checkpoint, load_processed_ids

SOURCE_COLLECTION = 'raw_data'  # Collection chứa dữ liệu gốc
TARGET_COLLECTION = 'product_names'  # Collection mới để chứa kết quả
BATCH_SIZE = 100
WORKERS = 15
MAX_RETRY_ROUNDS = 3

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
    "chrome124",
    "edge101",
    "safari17_0"
]

# Lấy thư mục file hiện tại
current_dir = os.path.dirname(__file__)

# Lấy thư mục gốc của project
project_dir = os.path.abspath(os.path.join(current_dir, '../..'))

# Lấy thư mục file .env
env_path = os.path.join(project_dir, '.env')

# Load file, nếu không thấy file sẽ báo
if load_dotenv(dotenv_path=env_path):
    print("Đã load thành công cấu hình từ file .env")
else:
    print("Không tìm thấy file .env, đang sử dụng biến môi trường có sẵn của hệ thống.")

product_name_path = os.environ.get('PRODUCT_NAME_PATH')

OUTPUT_CSV = get_filename(product_name_path, 'PRODUCT_NAME_PATH')

def get_total():
    # Hàm lấy tổng cộng bản ghi thỏa mãn điều kiện cần crawl
    db = get_database()
    src_collection = db[SOURCE_COLLECTION]

    # Chỉ lấy những field thực sự cần thiết
    # _id: 0 nghĩa là không lấy _id
    field_group = {
        "collection": 1,
        "product_id": 1,
        "viewing_product_id": 1,
        "_id": 0
    }

    # Lọc ngay từ đầu
    condition_group = {"collection": {"$in": GROUP1 + GROUP2}}

    # Lấy con trỏ (cursor) về, chưa tải data ngay
    cursor = src_collection.find(condition_group, field_group)

    products_set = set()

    for doc in cursor:
        productid = None
        col_type = doc.get("collection")

        if col_type in GROUP1:
            # Ưu tiên product_id, nếu không có thì lấy viewing_product_id
            productid = doc.get("product_id") or doc.get("viewing_product_id")
        else:
            # Lấy viewing_product_id
            productid = doc.get("viewing_product_id")

        if productid:
            products_set.add(productid)

    return len(products_set)

def get_product(existing_products_set):
    # Hàm lấy các dữ liệu product thỏa mãn điều kiện
    db = get_database()
    src_collection = db[SOURCE_COLLECTION]
    products_set = set()

    # Chuyển set các ID đã cào thành list để nhét vào query của MongoDB
    existing_list = list(existing_products_set)

    # Ép MongoDB lọc bỏ những ID đã cào
    condition_group1 = {
        "collection": {"$in": GROUP1},
        "product_id": {"$nin": existing_list},  # Không lấy ID đã cào
        "viewing_product_id": {"$nin": existing_list}  # Không lấy ID đã cào
    }

    field_group1 = {"product_id": 1, "viewing_product_id": 1, "current_url": 1}
    doc_group1 = src_collection.find(condition_group1, field_group1, no_cursor_timeout=True).batch_size(1000)

    try:
        for doc in doc_group1:
            product_id = doc.get('product_id') or doc.get('viewing_product_id')
            url = doc.get('current_url')

            # Kiểm tra và bỏ qua URL test ngay từ đầu
            if url and ("stage.glamira" in str(url) or "test.glamira" in str(url)):
                continue

            # LỌC CHECKPOINT: Bỏ qua nếu đã có trong file success productid
            if product_id and product_id in existing_products_set:
                continue

            if product_id and url and isinstance(url, str) and product_id not in products_set:
                products_set.add(product_id)
                yield {'product_id': product_id, 'url': url}
    finally:
        doc_group1.close()


    # Ép MongoDB lọc bỏ những ID đã cào
    condition_group2 = {
        "collection": {"$in": GROUP2},
        "viewing_product_id": {"$nin": existing_list}  # Không lấy ID đã cào
    }

    field_group2 = {"viewing_product_id": 1, "referrer_url": 1}
    doc_group2 = src_collection.find(condition_group2, field_group2, no_cursor_timeout=True).batch_size(1000)

    try:
        for doc in doc_group2:
            product_id = doc.get('viewing_product_id')
            url = doc.get('referrer_url')

            # Kiểm tra và bỏ qua URL test ngay từ đầu
            if url and ("stage.glamira" in str(url) or "test.glamira" in str(url)):
                continue

            # LỌC CHECKPOINT: Bỏ qua nếu đã có trong file success productid
            if product_id and product_id in existing_products_set:
                continue

            if product_id and url and isinstance(url, str) and product_id not in products_set:
                products_set.add(product_id)
                yield {'product_id': product_id, 'url': url}
    finally:
            doc_group2.close()

def name_scrapping(item):
    # Hàm crawl dữ liệu từ url và xử lý theo từng loại

    # Giúp request không bị gửi dồn dập cùng 1 lúc -> Server đỡ nghi ngờ
    # time.sleep(random.uniform(3, 5))

    product_id = item['product_id']
    url = item['url']

    session = requests.Session()

    # Chọn ngẫu nhiên 1 kiểu trình duyệt
    browser_type = random.choice(BROWSER_LIST)

    # Đây là những IP miễn phí và hạn mức sử dụng, hãy sử dụng IP của riêng mình
    proxy_list = [
        "http://qxpkccco:bfy3pd1qeiyi@31.59.20.176:6754",
        "http://qxpkccco:bfy3pd1qeiyi@23.95.150.145:6114",
        "http://qxpkccco:bfy3pd1qeiyi@198.23.239.134:6540",
        "http://qxpkccco:bfy3pd1qeiyi@45.38.107.97:6014",
        "http://qxpkccco:bfy3pd1qeiyi@107.172.163.27:6543",
        "http://qxpkccco:bfy3pd1qeiyi@198.105.121.200:6462",
        "http://qxpkccco:bfy3pd1qeiyi@64.137.96.74:6641",
        "http://qxpkccco:bfy3pd1qeiyi@216.10.27.159:6837",
        "http://qxpkccco:bfy3pd1qeiyi@142.111.67.146:5611",
        "http://qxpkccco:bfy3pd1qeiyi@23.26.53.37:6003"
    ]
    # Chọn ngẫu nhiên 1 cái proxy để dùng cho request này
    random_proxy = random.choice(proxy_list)
    proxies = {"http": random_proxy, "https": random_proxy}

    # Cấu hình số lần thử lại
    max_retry = 3

    for attempt in range(max_retry):
        try:
            res = session.get(
                url,
                timeout=10,
                impersonate=browser_type,
                proxies=proxies,
                verify=False
            )
            # TRƯỜNG HỢP 1: THÀNH CÔNG (200)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "lxml")
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

            # TRƯỜNG HỢP 2: LỖI 404 -> KHÔNG RETRY
            elif res.status_code == 404:
                return {'product_id': product_id, 'url': url, 'status': 'err_404',
                        'http_code': 404, 'error_msg': 'Page Not Found'}

            # TRƯỜNG HỢP 3: LỖI 403 -> RETRY và đổi sang browser khác để thử lại (tránh bị web chặn)
            elif res.status_code == 403:
                if attempt < max_retry - 1:
                    new_browser = random.choice([b for b in BROWSER_LIST if b != browser_type])
                    browser_type = new_browser
                    print(f"Gặp 403 (với productid {product_id}). Đổi sang {new_browser} và thử lại ngay...")

                    # Chỉ sleep nhẹ
                    time.sleep(30)
                    continue

                return {'product_id': product_id, 'url': url, 'status': 'err_403',
                        'http_code': 403, 'error_msg': 'Forbidden'}

            # TRƯỜNG HỢP 4: LỖI 429 -> SLEEP + RETRY
            elif res.status_code == 429:
                print('err_429')
                print(f"429 Rate Limit - ID {product_id}. Sleeping 5s... (Attempt {attempt + 1})")
                if attempt < max_retry - 1:
                    time.sleep(random.uniform(30, 60))
                    continue  # Quay lại đầu vòng lặp để thử lại

                return {'product_id': product_id, 'url': url, 'status': 'err_429',
                        'http_code': 429, 'error_msg': 'Rate limit'}

            # TRƯỜNG HỢP 5: LỖI 5xx -> RETRY
            elif res.status_code in range (500,600):
                # Đây chính là thay thế cho status_forcelist của HTTPAdapter
                print(f"Lỗi Server {res.status_code} - Retry...")
                if attempt < max_retry - 1:
                    time.sleep(2)
                    continue

                return {'product_id': product_id, 'url': url, 'status': 'err_5xx',
                        'http_code': res.status_code, 'error_msg': f'HTTP {res.status_code}'}

        except RequestsError as e:
            # LỖI MẠNG KHI REQUEST
            # print(f"Lỗi kết nối (Mất mạng/DNS): {e}")
            if attempt < max_retry - 1:
                time.sleep(2)
                continue
            return {'product_id': product_id, 'url': url, 'status': 'err_network', 'error_msg': str(e)}

        except Exception as e:
            # LỖI KHÁC
            if attempt < max_retry - 1:
                time.sleep(2)
                continue

            return {'product_id': product_id, 'url': url, 'status': 'err_other',
                    'http_code': 0, 'error_msg': str(e)}

    # Nếu hết vòng lặp mà vẫn dính lỗi khác
    return {'product_id': product_id, 'url': url, 'status': 'err_other',
            'http_code': res.status_code, 'error_msg': f'HTTP {res.status_code}'}


# DATA QUALITY (CHUẨN HÓA & VALIDATE)
def normalize_data(raw_item):
    # Hàm chuẩn hóa dữ liệu trước khi lưu
    return {
        'product_id': str(raw_item.get('product_id', '')).strip(),
        'product_name': str(raw_item.get('product_name', '')).strip(),
        'url': str(raw_item.get('url', '')).strip()
    }

def validate_data(item):
    # Hàm kiểm tra dữ liệu sạch
    if not item['product_id']:
        return False, 'missing_id'
    # Tên sản phẩm phải có và không được là "Unknown name"
    if not item['product_name'] or item['product_name'].lower() == 'unknown name':
        return False, 'missing_name'
    if not item['url']:
        return False, 'missing_url'
    return True, 'valid'

def process_results(results_list, csv_writer, stats):
    # Hàm xử lý danh sách kết quả (Dùng chung cho cả batch và phần dư)

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

        # 3. Xử lý CHECKPOINT: Ghi ngay lập tức productid này vào file txt tương ứng
        save_checkpoint(result['product_id'], result['status'])

        # 4. Xử lý Data Quality & chuẩn bị ghi vào MongoDB
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

def run_crawler_round(round_number, stats, existing_products_set, start_time):
    # Hàm xử lý crawl và lưu dữ liệu lặp lại theo từng round

    tgt_collection = db[TARGET_COLLECTION]

    products_list = []
    batch_start_time = time.time()

    print("\n" + "=" * 40)
    print(f"BẮT ĐẦU CRAWLING ROUND {round_number}")
    print(f"{'Chunk':<6} | {'Pending':<8} | {'OK':<6} | {'Valid':<6} | {'404':<5} | {'403':<5} | {'429':<5} | {'5xx':<5} | {'Net':<5} | {'Other':<5} | {'Time (s)':<10}")
    print("-" * 85)
    print(f"{0:<6} | {stats['total']:<8} | {stats['success']:<6} | {stats['dq_valid']:<6} | {stats['err_404']:<5} | {stats['err_403']:<5} | {stats['err_429']:<5} | {stats['err_5xx']:<5} | {stats['err_network']:<5} | {stats['err_other']:<5} | 0s")

    with open(OUTPUT_CSV, mode='a', encoding='utf-8-sig', newline='') as f:
        field_names = ['product_id', 'product_name', 'url', 'status', 'http_code', 'error_msg']

        writer = csv.DictWriter(f, fieldnames=field_names)

        # Nếu file chưa tồn tại thì ghi Header
        if not os.path.isfile(OUTPUT_CSV):
            # Ghi header
            writer.writeheader()

        # Sử dụng concurrent.futures để có nhiều luồng thu thập crawl dữ liệu hơn
        # Khởi tạo bể chứa các luồng
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
            for item in get_product(existing_products_set):
                products_list.append(item)

                if len(products_list) >= BATCH_SIZE:
                    results = list(executor.map(name_scrapping, products_list))

                    # Gọi hàm xử lý trung tâm
                    insert_mongo_batch = process_results(results, writer, stats)

                    # Insert vào Mongo (Chỉ data sạch)
                    if insert_mongo_batch:
                        try:
                            tgt_collection.insert_many(insert_mongo_batch, ordered=False)
                        except Exception as e:
                            print(f"Đây là lỗi khi insert vào MONGO: {e}")

                    # Xử lý LOGGING
                    current_time = time.time()
                    batch_duration = current_time - batch_start_time
                    batch_start_time = current_time

                    chunk_num = stats['processed'] // BATCH_SIZE
                    pending = stats['total'] - stats['processed']

                    print(f"{chunk_num:<6} | {pending:<8} | {stats['success']:<6} | {stats['dq_valid']:<6} | {stats['err_404']:<5} | {stats['err_403']:<5} | {stats['err_429']:<5} | "
                        f"{stats['err_5xx']:<5} | {stats['err_network']:<5} | {stats['err_other']:<5} | {batch_duration:.2f}s")

                    products_list = []  # Reset batch đầu vào

            # Ghi nốt số dữ liệu còn dư
            if products_list:
                results = list(executor.map(name_scrapping, products_list))

                insert_mongo_batch = []

                # Gọi hàm xử lý trung tâm
                insert_mongo_batch = process_results(results, writer, stats)

                # Insert vào Mongo (Chỉ data sạch)
                if insert_mongo_batch:
                    try:
                        tgt_collection.insert_many(insert_mongo_batch, ordered=False)
                    except Exception as e:
                        print(f"Đây là lỗi khi insert vào MONGO: {e}")

                # Xử lý LOGGING lô cuối
                current_time = time.time()
                batch_duration = current_time - batch_start_time
                chunk_num = (stats['processed'] // BATCH_SIZE) + 1
                pending = 0

                print(
                    f"{chunk_num:<6} | {pending:<8} | {stats['success']:<6} | {stats['dq_valid']:<6} | {stats['err_404']:<5} | {stats['err_403']:<5} | {stats['err_429']:<5} | {stats['err_5xx']:<5} | "
                    f"{stats['err_network']:<5} | {stats['err_other']:<5} | {batch_duration:.2f}s")

    total_duration = time.time() - start_time

    # --- FINAL REPORT ---
    print("\n" + "=" * 40)
    print(f"HOÀN TẤT CRAWLING ROUND {round_number}!")
    print(f"BÁO CÁO TỔNG KẾT")
    print("-" * 50)
    print(f"Tổng thời gian  : {total_duration:.2f}s ({total_duration / 60:.2f} phút)")
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
    print(f"Network Err     : {stats['err_network']}")
    print(f"Other Errors    : {stats['err_other']}")
    print("-" * 50)
    print(f"MongoDB: Đã lưu vào collection: '{TARGET_COLLECTION}'")
    print(f"CSV: Đã lưu vào '{OUTPUT_CSV}'")

if __name__ == "__main__":
    db = get_database()

    start_time = time.time()

    src_collection = db[SOURCE_COLLECTION]

    print("--> Đang kiểm tra và tạo Index tối ưu (nếu chưa tồn tại)...")
    # Tạo Compound Index: Giúp MongoDB lấy dữ liệu trực tiếp từ Index (RAM)
    src_collection.create_index([
        ("collection", 1),
        ("product_id", 1),
        ("viewing_product_id", 1)
    ])

    # --- BIẾN THỐNG KÊ CHI TIẾT ---
    stats = {
        'total': 0,  # Tổng số bản ghi
        'processed': 0,  # Số bản ghi đã xử lý
        'success': 0,  # Số bản ghi thành công
        'err_404': 0,  # Lỗi 404
        'err_403': 0,  # Lỗi 403
        'err_429': 0,  # Lỗi 429
        'err_5xx': 0,  # Lỗi 5xx
        'err_network': 0,  # Lỗi mất mạng
        'err_other': 0,  # Lỗi khác (Timeout, DNS...),
        # Data quality checks
        'dq_valid': 0,
        'dq_missing_id': 0,
        'dq_missing_name': 0,
        'dq_missing_url': 0
    }

    # VÒNG LẶP TOÀN CỤC (GLOBAL RETRY)
    for i in range(1, MAX_RETRY_ROUNDS + 1):
        # Đọc tất cả ID đã làm xong từ trước
        existing_products_set = load_processed_ids()

        stats['total'] = get_total() - len(existing_products_set)

        run_crawler_round(i, stats, existing_products_set, start_time)

        print(f"\n--> Kết thúc Vòng {i}.")
        if i < MAX_RETRY_ROUNDS:
            print("--> Nghỉ 10s trước khi quét lại các mục bị sót/lỗi...\n")
            time.sleep(10)
        else:
            print("--> Đã hết số lần thử lại (Max Retries). Dừng chương trình.\n")

    close_connection()
    print(f"Tổng thời gian: {time.time() - start_time:.2f}s ({(time.time() - start_time) / 60:.2f} phút)")