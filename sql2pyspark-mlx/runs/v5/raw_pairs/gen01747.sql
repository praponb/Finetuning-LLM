WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        region,
        sqft,
        PERCENT_RANK() OVER (ORDER BY sqft NULLS FIRST) AS sqft_percent_rank,
        CUME_DIST() OVER (PARTITION BY region ORDER BY sqft DESC) AS sqft_cume_dist,
        NTILE(10) OVER (PARTITION BY city ORDER BY sqft, id) AS sqft_bucket
    FROM stores
    WHERE region != 'East'
)
SELECT
    id,
    region,
    sqft_percent_rank,
    sqft_cume_dist,
    sqft_bucket,
    CASE WHEN region RLIKE '^(Central|South)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS region_zone
FROM scored
ORDER BY sqft_bucket DESC;
