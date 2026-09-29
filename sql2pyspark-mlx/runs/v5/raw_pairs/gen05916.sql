SELECT DATE_TRUNC('MONTH', o.order_date) order_month, ROUND(STDDEV(p.amount), 2) stddev_amount
FROM payments p
JOIN orders o ON p.order_id = o.id
GROUP BY DATE_TRUNC('MONTH', o.order_date)
HAVING ROUND(STDDEV(p.amount), 2) > 32
