WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(city) AS clean_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND rating < 3.7
)
-- Latest matching product per row (LATERAL ... LIMIT 1)
SELECT s.*, top1.product_launch_date
FROM cleaned s
LEFT JOIN LATERAL (
        SELECT c.launch_date AS product_launch_date
        FROM products c
        WHERE c.supplier_id = s.id
        ORDER BY c.cost DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
