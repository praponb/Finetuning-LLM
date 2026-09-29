WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.store_id = 1
        )
)
SELECT
    id,
    store_name,
    city,
    region,
    sqft,
    CASE WHEN store_name RLIKE '^(Riverside|Old Town)$' THEN 'PRIORITY' ELSE 'CORE' END AS store_name_zone
FROM eligible
