SELECT brand, category, COUNT(DISTINCT cost) AS unique_suppliers, SORT_ARRAY(COLLECT_SET(category)) AS category_list FROM products GROUP BY brand, category ORDER BY category, brand LIMIT 50;
