WITH cleaned AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        CEIL(amount) AS amount_whole
    FROM orders
    WHERE store_id = 24
)
SELECT channel
FROM cleaned
INTERSECT
SELECT channel
FROM orders
WHERE status = 'SHIPPED';
