SELECT
    CASE WHEN suppliers.country IN ('DE', 'US') THEN 'group_a' ELSE 'other' END AS country_group,
    CASE WHEN suppliers.rating >= 3.5 THEN 'high' WHEN suppliers.rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket,
    COUNT(DISTINCT products.stock_qty) AS unique_categorys,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(products.brand))) AS brand_values
FROM products
INNER JOIN suppliers ON suppliers.id = products.supplier_id
GROUP BY CASE WHEN suppliers.country IN ('DE', 'US') THEN 'group_a' ELSE 'other' END, CASE WHEN suppliers.rating >= 3.5 THEN 'high' WHEN suppliers.rating >= 1.8 THEN 'mid' ELSE 'low' END
ORDER BY brand_values, rating_bucket
