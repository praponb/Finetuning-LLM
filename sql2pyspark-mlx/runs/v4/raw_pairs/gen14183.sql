SELECT
    o.status,
    o.store_id,
    SUM(CASE WHEN p.status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    ROUND(STDDEV(p.amount), 2) AS stddev_amount
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
FULL OUTER JOIN customers c ON o.customer_id = c.id
WHERE p.paid_at < '2024-03-01'
GROUP BY o.status, o.store_id
ORDER BY 1, store_id DESC
LIMIT 1;
