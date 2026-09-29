SELECT s.*
FROM suppliers s
WHERE NOT EXISTS (
        SELECT 1
        FROM products c
        WHERE c.supplier_id = s.id
          AND c.supplier_id IN (7, 12, 26, 34)
    );
