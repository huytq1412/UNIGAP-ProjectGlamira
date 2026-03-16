import threading
import os
from dotenv import load_dotenv
from src.get_data_from_env import get_filename

# Lấy thư mục file hiện tại
current_dir = os.path.dirname(__file__)

# Lấy thư mục gốc của project
project_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Lấy thư mục file .env
env_path = os.path.join(project_dir, '.env')

load_dotenv(dotenv_path=env_path)

# SUCCESS_FILE_PATH = os.environ.get('SUCCESS_FILE_PATH')
# ERROR_404_FILE_PATH = os.environ.get('ERROR_404_FILE_PATH')
#
# # Tên file lưu trữ
# SUCCESS_FILE = get_filename(SUCCESS_FILE_PATH, 'SUCCESS_FILE_PATH')
# ERROR_404_FILE = get_filename(ERROR_404_FILE_PATH, 'ERROR_404_FILE_PATH')
#
# # Tạo khóa để tránh việc 2 luồng ghi file cùng lúc gây lỗi (Race condition)
# file_lock = threading.Lock()
#
# def load_processed_ids():
#     # Đọc toàn bộ ID đã crawl (thành công + 404) vào một Set.
#     processed_ids = set()
#
#     # 1. Đọc file thành công
#     if os.path.exists(SUCCESS_FILE):
#         with open(SUCCESS_FILE, 'r', encoding='utf-8') as f:
#             for line in f:
#                 processed_ids.add(line.strip())  # strip() để xóa xuống dòng
#
#     # 2. Đọc file 404 (Để sau này không cần crawl lại những link chết này)
#     if os.path.exists(ERROR_404_FILE):
#         with open(ERROR_404_FILE, 'r', encoding='utf-8') as f:
#             for line in f:
#                 processed_ids.add(line.strip())
#
#     print(f"--> Đã tải được {len(processed_ids)} ID đã xử lý từ trước (sẽ không thống kê lại những ID đã được xử lý khi chạy từ đầu).")
#     return processed_ids
#
# def save_checkpoint(product_id, status):
#     # Ghi ID vào file tương ứng ngay lập tức. Sử dụng Lock để an toàn khi sử dụng đa luồng.
#     filename = None
#
#     if status == 'success':
#         filename = SUCCESS_FILE
#     elif status == 'err_404':
#         filename = ERROR_404_FILE
#
#     # Chỉ ghi nếu là success hoặc 404
#     if filename:
#         with file_lock:  # Khóa lại, chỉ 1 luồng được ghi tại 1 thời điểm
#             with open(filename, 'a', encoding='utf-8') as f:
#                 f.write(f"{product_id}\n")
#                 # flush và fsync để đảm bảo dữ liệu được ghi xuống ổ cứng ngay lập tức chứ không nằm chờ trong bộ nhớ đệm (buffer)
#                 f.flush()
#                 os.fsync(f.fileno())

SUCCESS_FILE_PATH = os.environ.get('SUCCESS_FILE_PATH')
ERROR_404_FILE_PATH = os.environ.get('ERROR_404_FILE_PATH')
ERROR_403_FILE_PATH = os.environ.get('ERROR_403_FILE_PATH') # Thêm dòng này

SUCCESS_FILE = get_filename(SUCCESS_FILE_PATH, 'SUCCESS_FILE_PATH')
ERROR_404_FILE = get_filename(ERROR_404_FILE_PATH, 'ERROR_404_FILE_PATH')
ERROR_403_FILE = get_filename(ERROR_403_FILE_PATH, 'ERROR_403_FILE_PATH') # Thêm dòng này

file_lock = threading.Lock()

def load_processed_ids():
    # Chỉ load ID của Success và 404 (Chết vĩnh viễn)
    processed_ids = set()
    if os.path.exists(SUCCESS_FILE):
        with open(SUCCESS_FILE, 'r', encoding='utf-8') as f:
            for line in f: processed_ids.add(line.strip())
    if os.path.exists(ERROR_404_FILE):
        with open(ERROR_404_FILE, 'r', encoding='utf-8') as f:
            for line in f: processed_ids.add(line.strip())
    print(f"--> Đã tải được {len(processed_ids)} ID (thành công + lỗi 404) đã xử lý từ trước (sẽ không thống kê lại những ID đã được xử lý khi chạy từ đầu).")
    return processed_ids

def load_processed_403():
    # Load cặp (product_id, url) bị lỗi 403 vào một Set.
    error_403_pairs = set()
    if os.path.exists(ERROR_403_FILE):
        with open(ERROR_403_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(',', 1) # Tách ID và URL bằng dấu phẩy
                if len(parts) == 2:
                    error_403_pairs.add((parts[0], parts[1]))
    print(f"--> Đã tải {len(error_403_pairs)} cặp (ID, URL) bị lỗi 403 từ trước (sẽ bỏ qua các cặp này).")
    return error_403_pairs

def save_checkpoint(product_id, status, url=""):
    """
    Nhận thêm tham số url. Nếu là 403 thì ghi cả cặp.
    """
    filename = None
    if status == 'success':
        filename = SUCCESS_FILE
    elif status == 'err_404':
        filename = ERROR_404_FILE
    elif status == 'err_403':
        filename = ERROR_403_FILE

    if filename:
        with file_lock:
            with open(filename, 'a', encoding='utf-8') as f:
                if status == 'err_403':
                    f.write(f"{product_id},{url}\n") # Ghi thành cặp
                else:
                    f.write(f"{product_id}\n")
                f.flush()
                os.fsync(f.fileno())