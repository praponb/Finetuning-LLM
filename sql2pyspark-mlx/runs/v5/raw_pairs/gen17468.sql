WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        CUME_DIST() OVER (PARTITION BY region ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist,
        AVG(sqft) OVER (PARTITION BY city) AS city_avg_sqft
    FROM stores
),
enriched_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY city_avg_sqft, id) AS event_seq,
        NTILE(5) OVER w_ordered AS sqft_cume_dist_bucket
    FROM enriched
    WHERE city = 'New York'
    WINDOW w_ordered AS (ORDER BY sqft_cume_dist DESC, id)
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.inventory_quantity,
        latest.last_restock
    FROM enriched_stores s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY city_avg_sqft DESC NULLS LAST, id) = 1
),
enriched_stores_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(event_seq) OVER (PARTITION BY region ORDER BY event_seq, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS event_seq_avg_2_prior,
        STDDEV_SAMP(city_avg_sqft) OVER (PARTITION BY region, city) AS region_stddev_samp_city_avg_sqft,
        FIRST_VALUE(region, TRUE) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        FIRST_VALUE(sqft_cume_dist_bucket) IGNORE NULLS OVER (PARTITION BY region ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft_cume_dist_bucket,
        NTILE(4) OVER (ORDER BY sqft_cume_dist_bucket, id) AS sqft_cume_dist_bucket_bucket
    FROM ranked
),
kept_keys AS (
    -- Set difference on keys
    SELECT region, city
    FROM enriched_stores_2
    EXCEPT
    SELECT region, city
    FROM stores
    WHERE sqft > 12845
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_stores_2 s
    JOIN kept_keys k ON s.region = k.region AND s.city = k.city
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
    CASE WHEN sqft_cume_dist_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS sqft_cume_dist_bucket_tier,
    COUNT(*) FILTER (WHERE region = 'Central') AS central_rows,
    ROUND(SUM(inventory_quantity) / NULLIF(COUNT(CASE WHEN region = 'North' THEN 1 END), 0), 2) AS inventory_quantity_per_north,
    ROUND(SUM(city_avg_sqft) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS city_avg_sqft_per_chiang_mai
FROM kept_rows
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', last_restock), CASE WHEN sqft_cume_dist_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('QUARTER', last_restock)));
