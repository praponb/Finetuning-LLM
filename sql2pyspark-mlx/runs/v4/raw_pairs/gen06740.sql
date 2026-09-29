SELECT brand, SORT_ARRAY(COLLECT_SET(category)) AS category_list FROM products GROUP BY brand ORDER BY brand;
