import os
from sqlalchemy.exc import DatabaseError

def get_filename(raw_path, key):
    # Kiểm tra đường dẫn truyền vào None. Tạo thư mục nếu chưa có và trả về đường dẫn tuyệt đối.

    # Kiểm tra đường dẫn None/Empty
    if not raw_path:
        raise ValueError(f"LỖI: Biến {key} chưa được set hoặc bị rỗng trong file .env")

    # Lấy dường dẫn đầy đủ
    expanded_path = os.path.expanduser(raw_path)

    # Chuyển thành đường dẫn tuyệt đối để tránh lỗi khi chạy từ thư mục khác
    abs_path = os.path.abspath(expanded_path)

    # XỬ LÝ THƯ MỤC CHỨA FILE
    # Lấy đường dẫn thư mục cha
    folder_path = os.path.dirname(abs_path)

    # Kiểm tra xem thư mục này có tồn tại không
    if not os.path.exists(folder_path):
        print(f"Thư mục chưa tồn tại. Đang tạo: {folder_path}")
        try:
            os.makedirs(folder_path, exist_ok=True)
        except OSError as e:
            raise OSError(f"Lỗi: Không thể tạo thư mục '{folder_path}'. Chi tiết: {e}")
        except DatabaseError as e:
            raise DatabaseError(f"Lỗi từ database. Chi tiết: {e}")
        except Exception as e:
            raise RuntimeError(f"Lỗi không xác định: {e}")

    return abs_path
