WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price, id) AS row_num
        FROM products
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    brand,
    launch_date,
    tags,
    row_num,
    CASE WHEN brand RLIKE '^(Globex|Umbrella)$' THEN 'PRIORITY' WHEN brand RLIKE '^(Acme|Initech)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS brand_zone
FROM deduped
ORDER BY row_num DESC
