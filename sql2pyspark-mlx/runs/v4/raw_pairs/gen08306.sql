SELECT QUARTER(order_date) AS order_quarter, MAX(order_date) AS latest_order_date, ROUND((100 * SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_store
FROM orders
WHERE status = 'CANCELLED'
GROUP BY QUARTER(order_date)
ORDER BY pct_store, order_quarter
LIMIT 3;
