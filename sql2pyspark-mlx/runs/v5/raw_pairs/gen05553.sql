WITH with_dims AS (
    -- Attach stores attributes
    SELECT a.*, d.sqft
    FROM store_sales a
    LEFT JOIN stores d
        ON a.store_id = d.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.city,
        b.store_name,
        b.opened_date
    FROM with_dims e
    INNER JOIN stores b
        ON e.store_id = b.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        sale_date,
        sqft,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sale_date DESC, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY product_id, store_id ORDER BY units DESC, id) AS units_bucket,
        CORR(units, revenue) OVER (PARTITION BY store_id) AS corr_units_revenue
    FROM enriched
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CEIL(units_bucket) AS units_bucket_whole
    FROM scored
    WHERE sale_date > DATE '2023-06-04'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        sale_date,
        seq_no,
        units_bucket,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY units_bucket_whole DESC, id) AS units_bucket_whole_bucket,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY corr_units_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        CUME_DIST() OVER (ORDER BY corr_units_revenue DESC NULLS FIRST) AS corr_units_revenue_cume_dist,
        LAST_VALUE(units_bucket_whole, TRUE) OVER (PARTITION BY store_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units_bucket_whole
    FROM valid
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        sale_date,
        seq_no,
        units_bucket,
        units_bucket_whole_bucket,
        first_known_sqft,
        seq_no * first_known_sqft AS seq_no_times_first_known_sqft
    FROM metrics
    WHERE sale_date > DATE '2023-09-24'
      AND units_bucket BETWEEN 3 AND 7
),
scored_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(seq_no_times_first_known_sqft) OVER (ORDER BY units_bucket_whole_bucket, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS seq_no_times_first_known_sqft_avg_2_prior,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) AS event_seq,
        CUME_DIST() OVER (ORDER BY units_bucket DESC) AS units_bucket_cume_dist,
        NTILE(3) OVER (ORDER BY units_bucket_whole_bucket, id) AS units_bucket_whole_bucket_bucket
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    PERCENTILE_APPROX(first_known_sqft, 0.95) AS first_known_sqft_p95,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored_store_sales
GROUP BY 1
ORDER BY year_start DESC NULLS FIRST;
