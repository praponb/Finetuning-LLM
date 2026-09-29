SELECT s.*, top1.product_cost
FROM suppliers s
JOIN LATERAL (
        SELECT c.cost AS product_cost
        FROM products c
        WHERE c.supplier_id = s.id
        ORDER BY c.launch_date DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
