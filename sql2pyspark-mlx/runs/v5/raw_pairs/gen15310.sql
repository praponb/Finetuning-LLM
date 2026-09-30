SELECT CASE WHEN s.sqft >= 13835 THEN 'high' WHEN s.sqft >= 3719 THEN 'mid' ELSE 'low' END AS sqft_bucket, COUNT(o.discount) AS discount_count
FROM orders o
JOIN customers c ON o.customer_id = c.id
LEFT OUTER JOIN stores s ON o.store_id = s.id
GROUP BY CASE WHEN s.sqft >= 13835 THEN 'high' WHEN s.sqft >= 3719 THEN 'mid' ELSE 'low' END
ORDER BY 1
LIMIT 5;
