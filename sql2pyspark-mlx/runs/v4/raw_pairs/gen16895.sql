WITH base AS (
    SELECT *, LENGTH(status) AS status_length
    FROM orders
    WHERE order_date > DATE '2024-02-26'
      AND channel IS NOT NULL
)
SELECT *
FROM (
    SELECT
        store_id,
        status,
        shipping_fee
    FROM base
) src
PIVOT (
    AVG(shipping_fee) AS total,
    COUNT(1) AS item_count
    FOR status IN ('RETURNED', 'PENDING', 'CANCELLED')
);
