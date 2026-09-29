SELECT s.*, x.stock_qty
FROM suppliers s
JOIN LATERAL (
        SELECT c.stock_qty
        FROM products c
        WHERE c.supplier_id = s.id
        ORDER BY c.cost DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
