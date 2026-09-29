WITH rollup_totals AS (
    -- Join a dimension and aggregate
    SELECT
        dim.region,
        dim.city,
        COUNT(*) AS rows_total,
        AVG(a.shipping_fee) AS avg_shipping_fee,
        GROUPING_ID() AS grouping_level
    FROM orders a
    INNER JOIN stores dim ON a.store_id = dim.id
    GROUP BY GROUPING SETS ((dim.region, dim.city), (dim.region), ())
)
SELECT
    region,
    city,
    rows_total,
    avg_shipping_fee,
    grouping_level,
    CASE WHEN city RLIKE '^(Tokyo|Berlin)$' THEN 'CORE' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS city_zone
FROM rollup_totals
ORDER BY region DESC NULLS LAST;
