WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(unit_price) AS unit_price_whole,
        COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        quantity,
        unit_price_whole,
        LAST_VALUE(unit_price_whole) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY unit_price_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_whole,
        RANK() OVER (PARTITION BY product_id ORDER BY unit_price NULLS LAST) AS unit_price_rank,
        AVG(unit_price_whole) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS unit_price_whole_avg_6_prior
    FROM filtered
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        unit_price_whole,
        unit_price_rank,
        AVG(unit_price_whole_avg_6_prior) OVER (ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS unit_price_whole_avg_6_prior_avg_5_prior,
        SUM(last_known_unit_price_whole) OVER (ORDER BY unit_price_whole_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_unit_price_whole,
        NTILE(5) OVER (ORDER BY last_known_unit_price_whole, id) AS last_known_unit_price_whole_bucket
    FROM enriched
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        unit_price_whole,
        unit_price_rank,
        cumulative_last_known_unit_price_whole,
        last_known_unit_price_whole_bucket,
        ROW_NUMBER() OVER (ORDER BY cumulative_last_known_unit_price_whole DESC, id) AS event_seq,
        FIRST_VALUE(last_known_unit_price_whole_bucket, TRUE) OVER (ORDER BY unit_price_whole_avg_6_prior_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_known_unit_price_whole_bucket,
        AVG(unit_price_whole) OVER (ORDER BY unit_price_whole_avg_6_prior_avg_5_prior, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS unit_price_whole_avg_6,
        LEAD(quantity, 1) OVER (ORDER BY last_known_unit_price_whole_bucket, id) AS next_quantity,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
    FROM metrics
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(last_known_unit_price_whole_bucket, unit_price_whole_avg_6) AS least_last_known_unit_price_whole_bucket_unit_price_whole_avg_6,
        unit_price_rank * first_known_last_known_unit_price_whole_bucket AS unit_price_rank_times_first_known_last_known_unit_price_whole_bucket
    FROM scored
    WHERE unit_price_whole = 50
)
-- Grouped metrics
SELECT
    CASE WHEN last_known_unit_price_whole_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS last_known_unit_price_whole_bucket_tier,
    CASE WHEN next_quantity >= 8 THEN 'HIGH' ELSE 'LOW' END AS next_quantity_tier,
    CASE WHEN unit_price_whole_avg_6 >= 69 THEN 'HIGH' ELSE 'LOW' END AS unit_price_whole_avg_6_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(unit_price_whole), 2) AS avg_unit_price_whole
FROM cleaned
GROUP BY 1, 2, 3
