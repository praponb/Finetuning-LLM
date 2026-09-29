SELECT ROUND(AVG(discount), 2) AS avg_discount, SUM(CASE WHEN channel = 'web' THEN shipping_fee ELSE 0 END) AS web_shipping_fee, ROUND((100 * SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_store
FROM orders
WHERE channel = 'partner';
