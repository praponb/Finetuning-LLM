SELECT *, ARRAY_CONTAINS(tags, 'new') AS is_new
FROM products
