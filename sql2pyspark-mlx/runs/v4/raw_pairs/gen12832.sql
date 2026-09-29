SELECT
    ss.units,
    ss.sale_date,
    s.city,
    p.supplier_id,
    p.stock_qty
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
RIGHT JOIN products p ON ss.product_id = p.id;
