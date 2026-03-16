WITH stg_rate AS (
    SELECT 
        currency_code
        ,exchange_rate
        ,valid_from
        ,valid_to
    FROM {{ ref('stg_exchange_rates') }}
)

SELECT 
    -- Khóa chính: Kết hợp mã tiền tệ và ngày bắt đầu để đảm bảo tính duy nhất
    {{ dbt_utils.generate_surrogate_key(['currency_code', 'valid_from']) }} AS exchange_rate_key
    ,currency_code
    ,exchange_rate
    ,valid_from 
    -- Xử lý dòng mới nhất: Đổi NULL thành ngày 31/12/9999
    ,COALESCE(valid_to, CAST('9999-12-31' AS DATE)) AS valid_to
    -- đánh dấu phiên bản tỷ giá hiện tại đang có hiệu lực
    ,CASE 
        WHEN valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current

FROM stg_rate