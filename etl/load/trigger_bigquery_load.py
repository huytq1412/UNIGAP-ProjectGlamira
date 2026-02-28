import functions_framework
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATASET = "raw_layer"

# 1. Khởi tạo đối tượng BigQuery Client
try:
    # Kết nối với BigQuery xác thực bằng file json gcp_key
    client = bigquery.Client()

    logging.info(f"Đã kết nối BigQuery! Project ID đang dùng: {client.project}")
except Exception as e:
    logging.error(f"Lỗi khởi tạo kết nối: {e}")

@functions_framework.cloud_event
def trigger_bigquery_load(cloud_event):
    # 1. Detect new file in GCS
    data = cloud_event.data

    bucket_name = data["bucket"]

    # Lấy đầy đủ đường dẫn file (raw/raw_data/raw_data_part_xx.parquet)
    path_name = data["name"]

    if not path_name.endswith(".parquet"):
        logging.info("Skipping BigQuery load as it is not a parquet")
        return

    try:
        # 2. Start BigQuery load job
        # Lấy filename chuẩn (VD:raw_data_part_xx.parquet)
        file_name = path_name.split("/")[-1]

        # Lấy tên collection
        collection = file_name.split("_part_")[0]

        logging.info(f"\n========== ĐANG LOAD DỮ LIỆU BẢNG: {collection.upper()} ==========")

        # Đường dẫn Nguồn: Trỏ đến file parquet vừa kích hoạt trigger
        gcs_uri = f"gs://{bucket_name}/{path_name}"

        # Đường dẫn Đích
        target_table = f"{client.project}.{DATASET}.{collection}"

        # Bảng tạm lấy tên là: raw_data_staging_part_xx
        temp_table_name = file_name.replace('_part_', '_temp_part_').replace('.parquet', '')
        temp_table = f"{client.project}.{DATASET}.{temp_table_name}"

        # Kiểm tra có phải lần đầu load dữ liệu vào Bảng đích không
        try:
            target_table_ref = client.get_table(target_table)
            target_schema = target_table_ref.schema
            is_first_load = False
            logging.info(f"Bảng đích đã tồn tại. Sẽ ép kiểu dữ liệu theo bảng đích.")
        except NotFound:
            target_schema = None
            is_first_load = True
            logging.info(f"Bảng đích chưa tồn tại. Sẽ tự động nhận diện kiểu dữ liệu.")

        # Tạo LoadJob config
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # Tạo dữ liệu bảng temp mới
        )

        # Nếu không phải lần đầu, ép Schema của bảng tạm y hệt bảng đích
        if not is_first_load:
            job_config.schema = target_schema
        else:
            job_config.autodetect = True

        logging.info(f"Đang nạp {file_name} vào bảng đệm {temp_table}...")

        # Kích hoạt tiến trình LoadJob
        load_job = client.load_table_from_uri(
            gcs_uri,
            temp_table,
            job_config=job_config
        )

        load_job.result()

        if is_first_load:
            logging.info(f"Bảng đích chưa tồn tại. Đang khởi tạo bảng {target_table}...")

            # Tạo bảng đích bằng cách copy toàn bộ cấu trúc và dữ liệu từ bảng tạm
            create_query = f"CREATE TABLE `{target_table}` AS SELECT * FROM `{temp_table}`"

            query_job = client.query(create_query)
            query_job.result()
        else:
            logging.info("Đang đối chiếu _id và merge dữ liệu 2 bảng ...")

            merge_query = f"""
                        MERGE `{target_table}` tgt
                        USING `{temp_table}` tmp
                        ON tgt._id = tmp._id
                        WHEN NOT MATCHED THEN
                          INSERT ROW """

            query_job = client.query(merge_query)
            query_job.result()

        client.delete_table(temp_table, not_found_ok=True)
        logging.info(f"Đã dọn dẹp xong bảng tạm: {temp_table}")

        destination_table = client.get_table(target_table)
        logging.info(f"Tổng số dòng hiện tại của {collection} là: {destination_table.num_rows}.")

    except Exception as e:
        logging.error(f"Lỗi khi nạp bảng: {e}")



