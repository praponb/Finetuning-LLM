WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER w_recent AS units_bucket,
        NTILE(4) OVER (PARTITION BY product_id, store_id ORDER BY units DESC, id) AS units_bucket_2,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
        PERCENT_RANK() OVER w_part AS revenue_percent_rank,
        NTILE(4) OVER (ORDER BY units, id) AS units_bucket_3
    FROM store_sales
    WINDOW w_recent AS (PARTITION BY store_id ORDER BY units, id), w_part AS (PARTITION BY store_id ORDER BY revenue DESC)
),
scored_store_sales AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        units,
        revenue,
        units_share,
        revenue_percent_rank,
        AVG(revenue_percent_rank) OVER w_ordered AS last7_avg_revenue_percent_rank,
        COUNT(revenue_percent_rank) OVER w_ordered AS last7_count_revenue_percent_rank,
        MIN(revenue_percent_rank) OVER w_ordered AS last7_min_revenue_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY revenue, id) AS event_seq
    FROM scored
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY units_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        units,
        revenue,
        units_share,
        last7_min_revenue_percent_rank,
        event_seq,
        FLOOR(last7_count_revenue_percent_rank) AS last7_count_revenue_percent_rank_whole
    FROM scored_store_sales
    WHERE last7_avg_revenue_percent_rank > 0.54
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY last7_count_revenue_percent_rank_whole, id) AS seq_no,
        AVG(units) OVER (ORDER BY last7_count_revenue_percent_rank_whole, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS units_avg_5_prior,
        SUM(units_share) OVER (ORDER BY last7_count_revenue_percent_rank_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units_share
    FROM valid
),
scored_store_sales_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(last7_min_revenue_percent_rank) OVER (ORDER BY units_share, id) AS prev_last7_min_revenue_percent_rank,
        LEAD(last7_min_revenue_percent_rank) OVER (ORDER BY units_share, id) AS next_last7_min_revenue_percent_rank,
        LAG(units) OVER (ORDER BY units_share, id) AS prev_units,
        LEAD(units) OVER (ORDER BY units_share, id) AS next_units,
        ROW_NUMBER() OVER (ORDER BY units_share, id) AS seq_no_2,
        NTILE(10) OVER (ORDER BY event_seq DESC, id) AS event_seq_bucket
    FROM with_windows
    WHERE units > 23
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    seq_no,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    APPROX_PERCENTILE(last7_min_revenue_percent_rank, 0.9) AS last7_min_revenue_percent_rank_p90,
    APPROX_PERCENTILE(last7_min_revenue_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS last7_min_revenue_percent_rank_quartiles,
    ROUND(AVG(last7_min_revenue_percent_rank), 2) AS avg_last7_min_revenue_percent_rank,
    ROUND(AVG(seq_no_2), 2) AS avg_seq_no_2
FROM scored_store_sales_2
GROUP BY CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END, seq_no
HAVING ROUND(AVG(last7_min_revenue_percent_rank), 2) >= 0
