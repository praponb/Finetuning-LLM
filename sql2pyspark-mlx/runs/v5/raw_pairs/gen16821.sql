SELECT
    id,
    status,
    discount,
    AVG(discount) OVER (PARTITION BY status) AS status_avg_discount
FROM orders;
