SELECT *, SORT_ARRAY(tags) AS sorted_tags, CASE WHEN brand = 'Initech' THEN 1 ELSE 0 END AS is_initech FROM products WHERE ARRAY_CONTAINS(tags, 'sale');
