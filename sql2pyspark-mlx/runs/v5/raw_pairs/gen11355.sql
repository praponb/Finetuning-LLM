WITH cleaned AS (
    SELECT
        *,
        SUBSTRING(region, 1, 3) region_prefix,
        COALESCE(region, 'OTHER') region_clean
    FROM stores
    WHERE region IS NOT NULL
      AND store_name LIKE '%Rive%'
)
SELECT *
FROM (
    SELECT
        region,
        city,
        sqft
    FROM cleaned
) src
PIVOT (
    SUM(sqft) AS total
    FOR city IN ('Berlin', 'Tokyo')
)
