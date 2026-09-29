SELECT CASE WHEN discount >= 0.3 THEN 'high' WHEN discount >= 0.15 THEN 'mid' ELSE 'low' END AS discount_bucket, SUM(shipping_fee) AS total_shipping_fee
FROM orders
GROUP BY discount_bucket
ORDER BY 1
LIMIT 3;
