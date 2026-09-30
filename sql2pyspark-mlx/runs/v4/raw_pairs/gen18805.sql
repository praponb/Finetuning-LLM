WITH not_flagged AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date < s.opened_date
        )
)
SELECT
    region,
    CASE WHEN sqft >= 15421 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    PERCENTILE_APPROX(sqft, ARRAY(0.25, 0.5, 0.75)) AS sqft_quartiles,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS sqft_per_berlin,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count,
    COUNT(DISTINCT region) AS distinct_regions
FROM not_flagged
GROUP BY CUBE (region, CASE WHEN sqft >= 15421 THEN 'PREMIUM' ELSE 'STANDARD' END);
