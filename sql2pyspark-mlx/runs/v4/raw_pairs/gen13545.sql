SELECT
    supplier_id,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values,
    AVG(cost) AS avg_cost,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
FROM products
WHERE category IN ('Books', 'Grocery')
GROUP BY supplier_id
ORDER BY avg_cost DESC, supplier_id ASC
LIMIT 1
