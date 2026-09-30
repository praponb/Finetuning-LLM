SELECT o.status, MAX(p.amount) AS max_amount, COUNT(CASE WHEN p.method = 'card' THEN 1 END) AS card_count
FROM payments p
LEFT OUTER JOIN orders o ON o.id = p.order_id
LEFT OUTER JOIN stores s ON s.id = o.store_id
GROUP BY o.status
HAVING MAX(p.amount) >= 570
ORDER BY card_count DESC, status;
