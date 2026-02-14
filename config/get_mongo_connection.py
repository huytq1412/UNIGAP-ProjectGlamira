from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Lấy thư mục file hiện tại
current_dir = os.path.dirname(__file__)

# Lấy thư mục gốc của project
project_dir = os.path.abspath(os.path.join(current_dir, '..'))

# Lấy thư mục file .env
env_path = os.path.join(project_dir, '.env')

load_dotenv(dotenv_path=env_path)

MONGO_URI = os.environ.get('MONGO_URI')
DB_NAME = os.environ.get('DB_NAME')

# Biến toàn cục để lưu kết nối (Singleton)
_client = None

def get_database():
    # Hàm đảm bảo chỉ có 1 kết nối MongoClient được tạo ra và sử dụng cho toàn bộ chương trình.
    global _client

    if _client is None:
        # maxPoolSize=50: Cho phép mở tối đa 50 kết nối con song song
        _client = MongoClient(MONGO_URI, maxPoolSize=50)
        print("--> Đã khởi tạo kết nối MongoDB mới.")

    # Trả về đối tượng Database để dùng luôn
    return _client[DB_NAME]

def close_connection():
    # Hàm đóng kết nối khi chương trình dừng hẳn
    global _client
    if _client:
        _client.close()
        _client = None
        print("--> Đã đóng kết nối MongoDB.")


if __name__ == "__main__":
    db = get_database()
    close_connection()