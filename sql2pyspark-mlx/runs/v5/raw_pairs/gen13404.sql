SELECT
    oi.unit_price,
    p.category,
    p.stock_qty,
    p.supplier_id
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id;
