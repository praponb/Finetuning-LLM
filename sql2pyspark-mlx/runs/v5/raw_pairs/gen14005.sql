WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        shipping_fee * discount AS shipping_fee_times_discount,
        LENGTH(status) AS status_length,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM orders
    WHERE order_date > TO_DATE('2023-05-01')
)
SELECT id
FROM valid
INTERSECT
SELECT id
FROM orders
WHERE channel NOT IN ('store', 'mobile');
