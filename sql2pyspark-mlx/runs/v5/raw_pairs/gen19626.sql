SELECT
    id,
    customer_id,
    amount,
    channel,
    order_date,
    ROUND(amount / SUM(amount) OVER (PARTITION BY customer_id), 3) AS amount_share,
    LEAD(amount, 1) OVER (PARTITION BY channel ORDER BY order_date, id) AS next_amount
FROM orders
