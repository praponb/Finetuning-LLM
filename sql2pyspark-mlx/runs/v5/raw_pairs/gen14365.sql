WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC NULLS LAST, id) AS latest_rank
        FROM store_sales
    ) t
    WHERE latest_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(revenue) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_revenue,
        COUNT(revenue) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_revenue,
        AVG(revenue) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue,
        MIN(revenue) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_revenue
    FROM deduped
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY revenue, id) AS revenue_bucket,
        ROW_NUMBER() OVER (ORDER BY revenue DESC, id) AS event_seq
    FROM scored
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        units,
        revenue,
        latest_rank,
        running_max_revenue,
        revenue_bucket,
        event_seq,
        LEAD(running_max_revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS next_running_max_revenue,
        LAG(running_max_revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_running_max_revenue,
        LEAD(units) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS next_units,
        LAG(units) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS seq_no,
        AVG(running_count_revenue) OVER (ORDER BY revenue_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_count_revenue_avg_5_prior
    FROM enriched
    WHERE revenue_bucket BETWEEN 4 AND 7
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(seq_no) AS seq_no_whole,
        event_seq * prev_running_max_revenue AS event_seq_times_prev_running_max_revenue
    FROM with_windows
    WHERE sale_date > DATE '2023-06-17'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    CASE WHEN next_units >= 43 THEN 'HIGH' ELSE 'LOW' END AS next_units_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(prev_running_max_revenue), 2) AS avg_prev_running_max_revenue,
    BIT_OR(units) AS units_bit_or,
    COUNT(DISTINCT id) AS distinct_ids_2,
    MIN(revenue_bucket) AS min_revenue_bucket,
    GROUPING_ID() AS grouping_level
FROM filtered
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', sale_date), CASE WHEN next_units >= 43 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('YEAR', sale_date)), ())
ORDER BY distinct_ids_2
