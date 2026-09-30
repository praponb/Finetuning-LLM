SELECT
    s.id,
    s.supplier_name,
    x.product_stock_qty,
    x.product_brand
FROM suppliers s
LEFT JOIN LATERAL (
        SELECT c.stock_qty AS product_stock_qty, c.brand AS product_brand
        FROM products c
        WHERE c.supplier_id = s.id
        ORDER BY c.cost DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
