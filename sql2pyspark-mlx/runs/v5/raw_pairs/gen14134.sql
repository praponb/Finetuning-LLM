WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        REPLACE(store_name, ' ', '_') AS store_name_slug,
        CONCAT_WS('-', city, store_name) AS city_store_name_key,
        COALESCE(region, 'DIRECT') AS resolved_region
    FROM stores
    WHERE region = 'West'
)
-- Window metrics over several different partitions
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2,
    NTILE(10) OVER (PARTITION BY city ORDER BY sqft, id) AS sqft_bucket,
    NTILE(5) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_bucket_2,
    DENSE_RANK() OVER (ORDER BY sqft DESC NULLS LAST) AS sqft_dense_rank
FROM filtered
WHERE city = 'Berlin';
