SELECT
    id,
    customer_id,
    store_id,
    status,
    channel,
    amount
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id, channel ORDER BY amount DESC) AS row_num
    FROM orders
) t
WHERE row_num = 1;
