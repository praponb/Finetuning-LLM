SELECT
    id,
    customer_id,
    order_date,
    amount,
    status,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS row_num
FROM orders
WHERE MONTH(order_date) IN (5, 7, 8);
