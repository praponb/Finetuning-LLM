WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        INITCAP(TRIM(region)) region_title,
        ROUND(sqft, 2) sqft_rounded
    FROM stores
    WHERE opened_date > DATE '2024-01-04'
      AND region IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(sqft_rounded / sqft, 2) sqft_rounded_per_sqft
    FROM valid
    WHERE opened_date > DATE '2023-08-23'
      AND sqft > 8099
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY sqft DESC, id) sqft_bucket,
        AVG(sqft_rounded) OVER (ORDER BY sqft_rounded_per_sqft, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) sqft_rounded_avg_3_prior,
        LAST_VALUE(sqft_rounded_per_sqft, TRUE) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_sqft_rounded_per_sqft,
        AVG(sqft_rounded) OVER (ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) sqft_rounded_avg_5
    FROM filtered
),
valid_stores AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        region_title,
        sqft_rounded_per_sqft,
        sqft_bucket,
        ROUND(sqft_rounded_avg_3_prior, 2) sqft_rounded_avg_3_prior_rounded
    FROM scored
    WHERE opened_date >= DATE '2023-10-08'
),
scored_stores AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft_bucket) OVER (ORDER BY sqft_rounded_per_sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) sqft_bucket_avg_3,
        NTILE(3) OVER (ORDER BY sqft_rounded_per_sqft, id) sqft_rounded_per_sqft_bucket,
        NTILE(3) OVER (ORDER BY sqft_bucket DESC, id) sqft_bucket_bucket
    FROM valid_stores
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(sqft_rounded_per_sqft, 1, 0) OVER (ORDER BY sqft_rounded_avg_3_prior_rounded, id) next_sqft_rounded_per_sqft,
        FIRST_VALUE(sqft_rounded_avg_3_prior_rounded) IGNORE NULLS OVER (ORDER BY sqft_rounded_per_sqft_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_sqft_rounded_avg_3_prior_rounded,
        LEAD(sqft_bucket_avg_3, 1) OVER w_ordered next_sqft_bucket_avg_3,
        AVG(sqft_bucket) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) sqft_bucket_avg_3_prior,
        AVG(sqft_bucket_avg_3) OVER (ORDER BY sqft_bucket, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) sqft_bucket_avg_3_avg_5
    FROM scored_stores
    WINDOW w_ordered AS (ORDER BY opened_date, id)
),
valid_stores_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        sqft_rounded_per_sqft,
        sqft_bucket,
        sqft_rounded_avg_3_prior_rounded,
        sqft_bucket_avg_3,
        sqft_rounded_per_sqft_bucket,
        next_sqft_bucket_avg_3,
        sqft_bucket_avg_3_prior,
        sqft_bucket_avg_3_avg_5,
        ROUND(sqft_rounded_per_sqft_bucket * 1.07, 2) sqft_rounded_per_sqft_bucket_scaled
    FROM enriched
    WHERE next_sqft_bucket_avg_3 BETWEEN 4 AND 6
)
SELECT
    id,
    opened_date,
    sqft_rounded_per_sqft,
    sqft_rounded_avg_3_prior_rounded,
    sqft_rounded_per_sqft_bucket,
    next_sqft_bucket_avg_3,
    sqft_bucket_avg_3_avg_5,
    sqft_rounded_per_sqft_bucket_scaled
FROM valid_stores_2
ORDER BY sqft_rounded_per_sqft_bucket NULLS LAST
