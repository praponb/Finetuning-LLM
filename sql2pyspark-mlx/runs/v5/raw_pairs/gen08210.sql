SELECT MONTH(order_date) AS order_month, SUM(shipping_fee) AS total_shipping_fee, ROUND(SUM(shipping_fee) * 100.0 / SUM(SUM(shipping_fee)) OVER (), 2) AS pct_of_total
FROM orders
GROUP BY MONTH(order_date)
ORDER BY order_month DESC
