SELECT status, YEAR(order_date) AS order_year, SUM(shipping_fee) AS total_shipping_fee
FROM orders
GROUP BY status, YEAR(order_date)
HAVING SUM(shipping_fee) >= 220
ORDER BY total_shipping_fee, status DESC, order_year
LIMIT 1
