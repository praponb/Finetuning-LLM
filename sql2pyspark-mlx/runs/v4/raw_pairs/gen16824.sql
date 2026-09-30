SELECT
    id,
    order_date,
    discount,
    channel,
    amount,
    SUM(discount) OVER (ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount, id) AS amount_row
FROM orders;
