SELECT
    p.city,
    e.status,
    COUNT(*) AS num_rows,
    SUM(e.discount) AS total_discount,
    MIN(e.amount) AS min_amount
FROM orders e
LEFT JOIN stores p ON e.store_id = p.id
GROUP BY CUBE (p.city, e.status);
