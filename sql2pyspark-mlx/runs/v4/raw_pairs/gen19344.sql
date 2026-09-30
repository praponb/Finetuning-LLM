WITH filtered AS (
    SELECT
        *,
        TRIM(store_name) AS clean_store_name,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM stores
    WHERE opened_date > TO_DATE('2023-08-19')
      AND region IS NOT NULL
      AND opened_date BETWEEN '2024-01-01' AND '2025-05-12'
),
scored AS (
    SELECT
        *,
        FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY region, city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sqft_avg_3
    FROM filtered
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft_avg_3 NULLS LAST, id) AS rn
        FROM scored
    ) x
    WHERE rn = 1
)
SELECT
    CASE WHEN sqft_avg_3 >= 12774 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_avg_3_tier,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN city = 'New York' THEN 1 END), 0), 2) AS sqft_per_new_york,
    COUNT(*) FILTER (WHERE region = 'East') AS east_rows,
    SIZE(COLLECT_SET(region)) AS num_regions
FROM ranked
GROUP BY 1
