SELECT s.city, SORT_ARRAY(COLLECT_SET(p.brand)) AS brand_list FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id WHERE s.rating > 3.6 GROUP BY s.city
