WITH enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.quantity,
        a.unit_price,
        d.order_date
    FROM order_items a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_price, 1) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS next_unit_price,
        LEAD(quantity, 2, 0) OVER (PARTITION BY product_id ORDER BY dedup_rank, id) AS next_quantity,
        DENSE_RANK() OVER w_hist AS quantity_dense_rank
    FROM ranked
    WINDOW w_hist AS (PARTITION BY product_id, order_id ORDER BY quantity NULLS LAST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(unit_price) OVER (PARTITION BY order_id ORDER BY order_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_price_avg_2_prior,
        NTILE(5) OVER (PARTITION BY order_id ORDER BY next_quantity, id) AS next_quantity_bucket,
        AVG(quantity_dense_rank) OVER (PARTITION BY product_id, order_id ORDER BY quantity_dense_rank, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_dense_rank_avg_5
    FROM metrics
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        next_unit_price,
        quantity_dense_rank,
        unit_price_avg_2_prior,
        next_quantity_bucket
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity_dense_rank_avg_5 DESC) AS rank_in_group
        FROM with_windows
    ) t
    WHERE rank_in_group = 1
),
with_windows_order_items AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY unit_price_avg_2_prior DESC NULLS FIRST) AS unit_price_avg_2_prior_percent_rank,
        SUM(quantity_dense_rank) OVER (ORDER BY next_quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_dense_rank
    FROM latest
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(unit_price_avg_2_prior_percent_rank / unit_price_avg_2_prior, 2) AS unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior
    FROM with_windows_order_items
    WHERE cumulative_quantity_dense_rank <= 28
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior >= 90 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    PERCENTILE_APPROX(unit_price_avg_2_prior, ARRAY(0.25, 0.5, 0.75)) AS unit_price_avg_2_prior_quartiles,
    ROUND(AVG(unit_price_avg_2_prior_percent_rank), 2) AS avg_unit_price_avg_2_prior_percent_rank,
    COUNT(DISTINCT id) AS distinct_ids_3,
    SUM(next_unit_price) AS total_next_unit_price
FROM cleaned
GROUP BY CASE WHEN unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior >= 90 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY unit_price_avg_2_prior_percent_rank_per_unit_price_avg_2_prior_tier
