import os
import threading

# Tên file lưu trữ
SUCCESS_FILE = '/home/huytq/UNIGAP/ProjectGlamira/data/processed/success_productid.txt'
ERROR_404_FILE = '/home/huytq/UNIGAP/ProjectGlamira/data/processed/error_404_productid.txt'

# Tạo khóa để tránh việc 2 luồng ghi file cùng lúc gây lỗi (Race condition)
file_lock = threading.Lock()

def load_processed_ids():
    """
    Đọc toàn bộ ID đã crawl (thành công + 404) vào một Set.
    Dùng Set để tìm kiếm cực nhanh (O(1)).
    """
    processed_ids = set()

    # 1. Đọc file thành công
    if os.path.exists(SUCCESS_FILE):
        with open(SUCCESS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                processed_ids.add(line.strip())  # strip() để xóa xuống dòng

    # 2. Đọc file 404 (Để sau này không cần crawl lại những link chết này)
    if os.path.exists(ERROR_404_FILE):
        with open(ERROR_404_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                processed_ids.add(line.strip())

    print(f"--> Đã tải được {len(processed_ids)} ID đã xử lý từ trước (sẽ không thống kê lại những ID đã được xử lý khi chạy từ đầu).")
    return processed_ids


def save_checkpoint(product_id, status):
    """
    Ghi ID vào file tương ứng ngay lập tức.
    Sử dụng Lock để an toàn khi sử dụng đa luồng.
    """
    filename = None

    if status == 'success':
        filename = SUCCESS_FILE
    elif status == 'err_404':
        filename = ERROR_404_FILE

    # Chỉ ghi nếu là success hoặc 404
    if filename:
        with file_lock:  # Khóa lại, chỉ 1 luồng được ghi tại 1 thời điểm
            with open(filename, 'a', encoding='utf-8') as f:
                f.write(f"{product_id}\n")
                # flush và fsync để đảm bảo dữ liệu được ghi xuống ổ cứng ngay lập tức chứ không nằm chờ trong bộ nhớ đệm (buffer)
                f.flush()
                os.fsync(f.fileno())