SELECT
    id,
    customer_id,
    order_date,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS row_num
    FROM orders
) AS ranked
WHERE row_num <= 3
ORDER BY order_date, status
