WITH dim_totals AS (
    -- Join a dimension and aggregate
    SELECT
        dim.category,
        COUNT(*) AS cnt,
        ROUND(AVG(s.quantity), 2) AS avg_quantity,
        GROUPING_ID() AS gid
    FROM inventory s
    INNER JOIN products dim ON s.product_id = dim.id
    GROUP BY CUBE (dim.category)
)
SELECT
    category,
    cnt,
    gid,
    CASE WHEN category RLIKE '^(Fashion|Sports)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS category_zone
FROM dim_totals
ORDER BY category DESC NULLS LAST
