SELECT
    s.id,
    s.city,
    t.brand_values,
    t.stddev_price
FROM suppliers s
INNER JOIN (SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values, ROUND(STDDEV(price), 2) AS stddev_price
    FROM products
    WHERE brand IN ('Acme', 'Umbrella', 'Globex')
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.stddev_price > 24
ORDER BY stddev_price DESC, id
LIMIT 5
