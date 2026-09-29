SELECT MONTH(o.order_date) AS order_month, CASE WHEN o.amount >= 1610 THEN 'high' WHEN o.amount >= 390 THEN 'mid' ELSE 'low' END AS amount_bucket, SUM(CASE WHEN p.method = 'cash' THEN p.amount ELSE 0 END) AS cash_amount
FROM payments p
JOIN orders o ON o.id = p.order_id
GROUP BY MONTH(o.order_date), CASE WHEN o.amount >= 1610 THEN 'high' WHEN o.amount >= 390 THEN 'mid' ELSE 'low' END
HAVING SUM(CASE WHEN p.method = 'cash' THEN p.amount ELSE 0 END) >= 74
