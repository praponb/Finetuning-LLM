SELECT CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 2 THEN 'mid' ELSE 'low' END AS quantity_bucket, COUNT(*) AS order_item_count FROM order_items GROUP BY 1
