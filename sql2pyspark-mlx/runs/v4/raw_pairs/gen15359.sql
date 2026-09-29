SELECT
    id,
    channel,
    order_date,
    discount,
    SUM(discount) OVER (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount
FROM orders;
