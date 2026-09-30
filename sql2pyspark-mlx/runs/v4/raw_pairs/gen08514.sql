WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        SUBSTRING(store_name, 1, 4) AS store_name_prefix,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE sqft > 8430
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
        NTILE(4) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior_2,
        LEAD(sqft, 2) OVER (PARTITION BY city, region ORDER BY opened_date, id) AS next_sqft,
        ROW_NUMBER() OVER (ORDER BY sqft, id) AS row_seq
    FROM filtered
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY next_sqft NULLS FIRST, id) = 1
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sqft,
    store_name_prefix,
    region_filled,
    sqft_avg_5_prior,
    sqft_avg_5_prior_2,
    CASE WHEN city RLIKE '^(Bangkok|New York)$' THEN 'GROUP_B' WHEN city RLIKE '^(Chiang Mai|Singapore)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS city_zone
FROM ranked
ORDER BY city DESC;
