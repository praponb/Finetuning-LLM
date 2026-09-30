SELECT
    s.id,
    s.city,
    top1.product_stock_qty,
    top1.price
FROM suppliers s
LEFT JOIN LATERAL (
        SELECT c.stock_qty AS product_stock_qty, c.price
        FROM products c
        WHERE c.supplier_id = s.id
        ORDER BY c.price DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
