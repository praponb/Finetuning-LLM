WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        revenue,
        COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE sale_date > DATE '2023-05-27'
      AND revenue < 3100
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(revenue) OVER (PARTITION BY store_id ORDER BY revenue_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_revenue,
        AVG(revenue) OVER (PARTITION BY store_id ORDER BY revenue_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_revenue,
        MIN(revenue) OVER (PARTITION BY store_id ORDER BY revenue_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_revenue
    FROM cleaned
),
metrics_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        revenue,
        last3_count_revenue,
        last3_min_revenue,
        SUM(revenue_filled) OVER (PARTITION BY store_id ORDER BY last3_count_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue_filled,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last3_count_revenue, id) AS seq_no,
        RANK() OVER w_grp AS revenue_filled_rank,
        MIN(last3_count_revenue) OVER (PARTITION BY store_id) AS store_id_min_last3_count_revenue
    FROM metrics
    WHERE last3_avg_revenue <= 3700
    WINDOW w_grp AS (PARTITION BY store_id ORDER BY revenue_filled DESC)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY revenue_filled_rank DESC, id) AS revenue_filled_rank_bucket,
        AVG(last3_count_revenue) OVER (PARTITION BY store_id ORDER BY seq_no, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS last3_count_revenue_avg_5,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last3_count_revenue DESC, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) AS seq_no_2
    FROM metrics_store_sales
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue_filled_rank DESC NULLS FIRST, id) AS rn
    FROM with_windows
    QUALIFY rn = 1
)
SELECT
    id,
    store_id,
    revenue,
    last3_count_revenue,
    last3_min_revenue,
    revenue_filled_rank,
    revenue_filled_rank_bucket,
    last3_count_revenue_avg_5,
    seq_no_2
FROM latest
ORDER BY last3_count_revenue_avg_5 DESC, last3_count_revenue;
