SELECT
    order_items.id,
    order_items.unit_price,
    products.product_name,
    products.stock_qty,
    products.supplier_id
FROM order_items
LEFT OUTER JOIN products ON order_items.product_id = products.id
WHERE products.supplier_id IS NULL
ORDER BY products.stock_qty DESC;
