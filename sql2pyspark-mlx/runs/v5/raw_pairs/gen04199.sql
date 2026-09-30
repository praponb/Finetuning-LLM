SELECT CASE WHEN o.discount >= 0.34 THEN 'high' WHEN o.discount >= 0.15 THEN 'mid' ELSE 'low' END AS discount_bucket, YEAR(o.order_date) AS order_year, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.status))) AS status_values
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE o.shipping_fee IS NOT NULL
GROUP BY CASE WHEN o.discount >= 0.34 THEN 'high' WHEN o.discount >= 0.15 THEN 'mid' ELSE 'low' END, YEAR(o.order_date)
ORDER BY discount_bucket DESC;
