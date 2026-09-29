SELECT stock_qty, supplier_id, cost
FROM products
WHERE SIZE(tags) >= 2 AND ARRAY_CONTAINS(tags, 'bestseller');
