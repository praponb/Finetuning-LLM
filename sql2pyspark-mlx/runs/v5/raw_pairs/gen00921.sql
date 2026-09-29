SELECT s.*
FROM suppliers s
WHERE NOT EXISTS (
        SELECT 1
        FROM products c
        WHERE c.supplier_id = s.id
          AND ARRAY_CONTAINS(c.tags, 'bestseller')
    );
