SELECT id, status, order_date
FROM (
    SELECT
        id,
        status,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_date DESC, id) AS row_num
    FROM orders
) AS t
WHERE row_num <= 5;
