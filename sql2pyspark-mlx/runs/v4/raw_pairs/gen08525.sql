WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        ship_to,
        attributes['utm_source'] AS utm_source,
        COALESCE(attributes['device'], 'direct') AS device_or_default,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        attributes['gift'] AS gift
    FROM carts
)
-- Running and moving aggregates
SELECT
    *,
    FIRST_VALUE(utm_source, TRUE) OVER (PARTITION BY customer_id ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_utm_source,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id) AS seq_no,
    ROW_NUMBER() OVER (ORDER BY created_at DESC, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id) AS row_seq_2,
    LAST_VALUE(device_or_default, TRUE) OVER (ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_device_or_default
FROM unpacked;
