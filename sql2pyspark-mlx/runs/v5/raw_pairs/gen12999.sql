WITH cleaned AS (
    SELECT *, COALESCE(brand, 'unassigned') AS brand_filled
    FROM products
    WHERE supplier_id IN (13, 34)
)
SELECT s.*
FROM cleaned s
WHERE EXISTS (
        SELECT 1
        FROM inventory c
        WHERE c.product_id = s.id
          AND c.last_restock < s.launch_date
    );
