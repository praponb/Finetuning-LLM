SELECT id, customer_id, order_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS row_num
    FROM orders
    WHERE order_date < '2024-08-01'
) AS ranked
WHERE row_num = 1
ORDER BY customer_id;
