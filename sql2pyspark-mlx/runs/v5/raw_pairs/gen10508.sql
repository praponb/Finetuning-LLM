SELECT DATE_TRUNC('MONTH', o.order_date) AS order_month, QUARTER(o.order_date) AS order_quarter, SUM(p.amount) AS total_amount
FROM payments p
RIGHT JOIN orders o ON o.id = p.order_id
GROUP BY DATE_TRUNC('MONTH', o.order_date), QUARTER(o.order_date)
HAVING SUM(p.amount) >= 2900
ORDER BY total_amount
