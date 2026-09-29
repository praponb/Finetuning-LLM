SELECT o.status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(p.method))) AS method_values, COUNT(DISTINCT p.amount) AS unique_methods
FROM payments p
JOIN orders o ON p.order_id = o.id
FULL OUTER JOIN customers c ON o.customer_id = c.id
WHERE o.channel NOT IN ('store', 'partner')
GROUP BY o.status
