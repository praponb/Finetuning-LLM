WITH filtered_suppliers AS (
    SELECT *, CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg
    FROM suppliers
    WHERE city IN ('Bangkok', 'Berlin', 'London')
),
summary AS (
    SELECT CASE WHEN is_sg >= 75 THEN 'high' WHEN is_sg >= 38 THEN 'mid' ELSE 'low' END AS is_sg_bucket, SUM(is_sg) AS total_is_sg
    FROM filtered_suppliers
    GROUP BY CASE WHEN is_sg >= 75 THEN 'high' WHEN is_sg >= 38 THEN 'mid' ELSE 'low' END
)
SELECT is_sg_bucket, total_is_sg
FROM summary
WHERE total_is_sg > 270
ORDER BY 2
