WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date >= s.launch_date
        )
)
SELECT
    id,
    category,
    brand,
    stock_qty,
    launch_date,
    supplier_id,
    CASE WHEN brand RLIKE '^(Initech|Globex)$' THEN 'OTHER_ZONE' WHEN brand RLIKE '^(Acme|Umbrella)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS brand_zone
FROM not_flagged
ORDER BY brand NULLS LAST;
