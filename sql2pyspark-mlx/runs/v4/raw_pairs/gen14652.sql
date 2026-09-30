WITH eligible AS (
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.opened_date
        )
)
SELECT
    CASE WHEN sqft >= 17893 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    city,
    SIZE(COLLECT_SET(region)) AS num_regions,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    COUNT(*) FILTER (WHERE region = 'West') AS west_rows
FROM eligible
GROUP BY CASE WHEN sqft >= 17893 THEN 'HIGH' ELSE 'LOW' END, city
ORDER BY sqft_tier;
