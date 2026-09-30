SELECT
    order_items.id,
    order_items.order_id,
    products.stock_qty,
    products.product_name,
    suppliers.rating,
    suppliers.city
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
INNER JOIN suppliers ON suppliers.id = products.supplier_id
ORDER BY products.stock_qty DESC, id
LIMIT 3;
