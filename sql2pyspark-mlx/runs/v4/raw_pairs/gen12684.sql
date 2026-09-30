WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date <= s.launch_date
        )
)
SELECT
    id,
    product_name,
    price,
    launch_date
FROM not_flagged
ORDER BY launch_date DESC NULLS FIRST, id ASC;
