SELECT MONTH(o.order_date) AS order_month, ROUND(STDDEV(p.amount), 2) AS stddev_amount
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
JOIN stores s ON o.store_id = s.id
GROUP BY MONTH(o.order_date)
ORDER BY stddev_amount DESC
