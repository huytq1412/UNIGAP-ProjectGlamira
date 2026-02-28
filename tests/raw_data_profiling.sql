--1. Data Types Review
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable
FROM
    `unigap-glamira-project.raw_layer.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_name IN ('raw_data', 'product_names', 'ip_locations')
ORDER BY
    table_name, ordinal_position;

--2. Data profiling
SELECT
    -- A. TỔNG QUAN
    COUNT(*) AS total_rows,

    -- B. KIỂM TRA RÀNG BUỘC KHÓA CHÍNH (_id)
    COUNT(DISTINCT _id) AS distinct_id_count,
    IF(COUNT(*) = COUNT(DISTINCT _id), 'Passed', 'Failed - Duplicated') AS is_id_unique,

    -- C. ĐẾM GIÁ TRỊ RỖNG (NULL) CỦA CÁC CỘT QUAN TRỌNG
    -- Kiểm tra user_id_db
    COUNTIF(user_id_db IS NULL OR user_id_db = '') AS null_user_id_db,
    ROUND(COUNTIF(user_id_db IS NULL OR user_id_db = '') / COUNT(*) * 100, 2) AS null_pct_user_id_db,

    -- Kiểm tra thiết bị
    COUNTIF(device_id IS NULL OR device_id = '') AS null_device_id,

    -- D. KHÁM ĐỘ PHỦ DỮ LIỆU (DISTINCT VALUES)
    COUNT(DISTINCT device_id) AS distinct_devices,
    COUNT(DISTINCT store_id) AS distinct_stores,
    COUNT(DISTINCT api_version) AS distinct_api_versions,

    -- E. KIỂM TRA TÍNH NHẤT QUÁN CỦA CỘT PRICE (Vì đang là STRING)
    -- Đếm xem có bao nhiêu dòng giá bị rỗng hoặc bằng 0
    COUNTIF(price IS NULL OR price = '' OR price = '0') AS zero_or_null_price_count

FROM
    `unigap-glamira-project.raw_layer.raw_data`;