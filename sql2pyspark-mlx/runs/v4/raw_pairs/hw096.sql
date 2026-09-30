SELECT id, ARRAY_CONTAINS(tags, 'sale') AS on_sale FROM products;
