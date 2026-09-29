SELECT s.country, SORT_ARRAY(COLLECT_SET(p.category)) AS category_list
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY s.country ASC
