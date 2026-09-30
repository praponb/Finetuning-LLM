WITH cleaned AS (
    SELECT *, CONCAT(store_name, '/', region) AS store_name_region
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date >= DATE '2023-03-25'
)
SELECT
    *,
    MAX(sqft) OVER w_grp AS last3_max_sqft,
    COUNT(sqft) OVER w_grp AS last3_count_sqft,
    MIN(sqft) OVER w_grp AS last3_min_sqft,
    NTILE(4) OVER (PARTITION BY city ORDER BY sqft, id) AS sqft_bucket
FROM cleaned
WHERE region IN ('South', 'Central', 'North')
WINDOW w_grp AS (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
