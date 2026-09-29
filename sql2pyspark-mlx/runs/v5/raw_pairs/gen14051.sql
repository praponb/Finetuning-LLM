WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft,
        NTILE(4) OVER (PARTITION BY city ORDER BY sqft, id) sqft_bucket,
        SUM(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft_2
    FROM stores
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(sqft, 2) OVER (PARTITION BY region ORDER BY sqft_bucket, id) next_sqft,
        NTILE(3) OVER (PARTITION BY region ORDER BY sqft_bucket, id) sqft_bucket_bucket
    FROM metrics
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY opened_date NULLS LAST) = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        sqft_bucket * sqft_bucket_bucket sqft_bucket_times_sqft_bucket_bucket,
        COALESCE(region, 'OTHER') region_clean
    FROM ranked
    WHERE next_sqft BETWEEN 4695 AND 12145
),
metrics_stores AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY next_sqft DESC, id) seq_no,
        NTILE(3) OVER (ORDER BY sqft_bucket_times_sqft_bucket_bucket DESC, id) sqft_bucket_times_sqft_bucket_bucket_bucket
    FROM valid
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        running_sqft,
        sqft_bucket,
        next_sqft,
        sqft_bucket_bucket,
        sqft_bucket_times_sqft_bucket_bucket,
        seq_no,
        sqft_bucket_times_sqft_bucket_bucket_bucket,
        LENGTH(city) city_length,
        COALESCE(region, 'UNKNOWN') resolved_region
    FROM metrics_stores
    WHERE opened_date >= DATE '2023-06-06'
      AND region IS NOT NULL
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_quantity,
        top1.inventory_last_restock
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    opened_date,
    sqft,
    running_sqft,
    sqft_bucket,
    sqft_bucket_bucket,
    sqft_bucket_times_sqft_bucket_bucket,
    seq_no,
    sqft_bucket_times_sqft_bucket_bucket_bucket
FROM with_latest;
