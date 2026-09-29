SELECT YEAR(o.order_date) AS order_year, MAX(p.amount) AS max_amount, MIN(p.amount) AS min_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
WHERE o.status <> 'PENDING'
GROUP BY YEAR(o.order_date)
