SELECT CASE WHEN suppliers.rating >= 4.1 THEN 'high' WHEN suppliers.rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(products.brand))) AS brand_values, SORT_ARRAY(COLLECT_SET(products.brand)) AS brand_list
FROM products
LEFT JOIN suppliers ON products.supplier_id = suppliers.id
WHERE products.product_name LIKE '%Lapto%'
GROUP BY CASE WHEN suppliers.rating >= 4.1 THEN 'high' WHEN suppliers.rating >= 1.8 THEN 'mid' ELSE 'low' END
