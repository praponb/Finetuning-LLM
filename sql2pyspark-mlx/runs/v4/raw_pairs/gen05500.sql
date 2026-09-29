WITH base AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        LOWER(status) AS status_lower,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM orders
    WHERE order_date >= DATE '2024-01-28'
      AND shipping_fee <= 10
)
SELECT id
FROM base
EXCEPT DISTINCT
SELECT id
FROM orders
WHERE status = 'RETURNED';
