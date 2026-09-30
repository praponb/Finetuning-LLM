WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(revenue) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_revenue,
        MIN(revenue) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_revenue,
        COUNT(revenue) OVER (PARTITION BY store_id, product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_revenue,
        SUM(revenue) OVER (PARTITION BY store_id, product_id) AS store_id_sum_revenue
    FROM store_sales
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        running_min_revenue,
        running_count_revenue,
        LAST_VALUE(store_id_sum_revenue, TRUE) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_store_id_sum_revenue,
        LAG(store_id_sum_revenue, 1) OVER (PARTITION BY product_id ORDER BY running_count_revenue, id) AS prev_store_id_sum_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_min_revenue DESC, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY store_id_sum_revenue, id) AS seq_no
    FROM metrics
),
with_windows_store_sales AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        running_min_revenue,
        last_known_store_id_sum_revenue,
        event_seq,
        seq_no,
        LEAD(seq_no, 1) OVER (PARTITION BY product_id ORDER BY seq_no, id) AS next_seq_no,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY event_seq DESC, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(row_seq) IGNORE NULLS OVER w_recent AS last_known_row_seq,
        SUM(running_min_revenue) OVER (PARTITION BY product_id ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_min_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS seq_no_2,
        SUM(running_min_revenue) OVER (PARTITION BY product_id ORDER BY next_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_min_revenue_2,
        NTILE(10) OVER w_hist AS seq_no_bucket
    FROM with_windows_store_sales
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY product_id ORDER BY seq_no DESC, id)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        sale_date,
        units,
        running_min_revenue,
        last_known_store_id_sum_revenue,
        event_seq,
        seq_no,
        next_seq_no,
        row_seq,
        last_known_row_seq,
        cumulative_running_min_revenue,
        seq_no_2,
        cumulative_running_min_revenue_2,
        seq_no_bucket,
        ROUND(seq_no_bucket, 0) AS seq_no_bucket_rounded
    FROM enriched
    WHERE next_seq_no > 8
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM valid
    INTERSECT
    SELECT id, product_id
    FROM store_sales
    WHERE units >= 13
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id AND s.product_id = k.product_id
)
-- Join a dimension and aggregate
SELECT
    dim.category,
    COUNT(*) AS num_rows,
    MAX(a.cumulative_running_min_revenue) AS max_cumulative_running_min_revenue
FROM kept_rows a
LEFT JOIN products dim ON a.product_id = dim.id
GROUP BY dim.category
