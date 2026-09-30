SELECT
    id,
    channel,
    order_date,
    discount,
    SUM(discount) OVER (PARTITION BY channel ORDER BY order_date, id) AS running_discount
FROM orders;
