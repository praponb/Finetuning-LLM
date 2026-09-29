WITH base AS (
    SELECT
        *,
        CASE WHEN region = 'South' THEN 1 ELSE 0 END AS is_south,
        UPPER(region) AS region_upper
    FROM stores
    WHERE opened_date >= DATE '2023-03-14'
      AND region IS NOT NULL
),
scored AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        is_south,
        region_upper,
        NTILE(3) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket,
        FIRST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        NTILE(3) OVER (PARTITION BY region, city ORDER BY is_south DESC, id) AS is_south_bucket
    FROM base
),
scored_stores AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        is_south,
        region_upper,
        sqft_bucket,
        first_known_region,
        sqft_share,
        is_south_bucket,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share_2,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_share DESC, id) AS seq_no
    FROM scored
)
SELECT
    id,
    store_name,
    city,
    region,
    sqft,
    is_south,
    region_upper,
    sqft_bucket,
    first_known_region,
    CASE WHEN region RLIKE '^(West|South)$' THEN 'GROUP_A' WHEN region RLIKE '^(North|East)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS region_zone
FROM scored_stores
ORDER BY city DESC NULLS LAST, sqft;
