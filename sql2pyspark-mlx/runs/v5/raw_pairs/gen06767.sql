WITH with_dims AS (
    SELECT
        src.*,
        o.channel,
        o.status AS order_status,
        o.shipping_fee
    FROM payments src
    LEFT JOIN orders o
        ON src.order_id = o.id
)
SELECT
    method,
    MIN(paid_at) AS first_seen,
    COUNT(*) FILTER (WHERE method = 'promptpay') AS promptpay_rows
FROM with_dims
GROUP BY method;
