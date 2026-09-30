SELECT
    id,
    customer_id,
    order_date,
    status,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date, id) AS row_num,
    RANK() OVER (PARTITION BY status ORDER BY order_date) AS rnk
FROM orders
WHERE YEAR(order_date) >= 2024
ORDER BY customer_id DESC;
