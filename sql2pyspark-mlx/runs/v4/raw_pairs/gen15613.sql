SELECT CASE WHEN o.discount >= 0.34 THEN 'high' WHEN o.discount >= 0.07 THEN 'mid' ELSE 'low' END AS discount_bucket, MIN(p.paid_at) AS min_paid_at, MAX(p.amount) - MIN(p.amount) AS amount_range
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
WHERE HOUR(p.paid_at) BETWEEN 6 AND 11
GROUP BY CASE WHEN o.discount >= 0.34 THEN 'high' WHEN o.discount >= 0.07 THEN 'mid' ELSE 'low' END;
