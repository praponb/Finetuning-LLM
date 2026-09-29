SELECT
    p.stock_qty,
    p.cost,
    p.product_name,
    ss.units
FROM products p
LEFT JOIN store_sales ss ON ss.product_id = p.id;
