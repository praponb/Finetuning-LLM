SELECT
    o.store_id,
    o.status,
    SUM(p.amount) AS total_amount,
    MAX(p.amount) AS max_amount
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON c.id = o.customer_id
GROUP BY o.store_id, o.status
ORDER BY total_amount DESC, store_id ASC, status ASC
LIMIT 5;
