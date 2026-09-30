SELECT *, stock_qty - cost AS stock_qty_minus_cost, ARRAY_CONTAINS(tags, 'new') AS is_new
FROM products
WHERE cost IS NOT NULL;
