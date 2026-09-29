WITH base AS (
    SELECT
        *,
        CASE WHEN sqft >= 16317 THEN 'High' WHEN sqft >= 7029 THEN 'Medium' ELSE 'Low' END AS sqft_level,
        TRIM(store_name) AS clean_store_name,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE sqft < 8252
)
SELECT *
FROM (
    SELECT
        city,
        region,
        sqft
    FROM base
) src
PIVOT (
    AVG(sqft) AS value_sum
    FOR region IN ('East' AS east, 'Central' AS central)
);
