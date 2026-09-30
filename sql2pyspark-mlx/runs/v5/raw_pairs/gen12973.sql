WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        DATE_TRUNC('month', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock <= '2025-06-01'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS row_seq
    FROM base
),
metrics_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last_restock,
        last_restock_period,
        SUM(row_seq) OVER w_ordered AS last4_sum_row_seq,
        MAX(row_seq) OVER w_ordered AS last4_max_row_seq,
        AVG(row_seq) OVER w_ordered AS last4_avg_row_seq,
        AVG(row_seq) OVER (PARTITION BY product_id ORDER BY row_seq, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_seq_avg_2_prior
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        last_restock_period,
        last4_sum_row_seq,
        last4_max_row_seq,
        last4_avg_row_seq,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM metrics_inventory
    WHERE HOUR(last_restock_period) BETWEEN 6 AND 11
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(last4_sum_row_seq) OVER w_hist AS running_count_last4_sum_row_seq,
        MIN(last4_sum_row_seq) OVER w_hist AS running_min_last4_sum_row_seq,
        SUM(last4_sum_row_seq) OVER w_hist AS running_sum_last4_sum_row_seq,
        AVG(last4_sum_row_seq) OVER w_hist AS running_avg_last4_sum_row_seq
    FROM cleaned
    WINDOW w_hist AS (ORDER BY last_restock_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        days_since_last_restock,
        running_count_last4_sum_row_seq,
        running_min_last4_sum_row_seq,
        running_sum_last4_sum_row_seq,
        LAG(running_min_last4_sum_row_seq) OVER (ORDER BY last4_avg_row_seq, id) AS prev_running_min_last4_sum_row_seq,
        LEAD(running_min_last4_sum_row_seq) OVER (ORDER BY last4_avg_row_seq, id) AS next_running_min_last4_sum_row_seq,
        LAG(running_sum_last4_sum_row_seq) OVER (ORDER BY last4_avg_row_seq, id) AS prev_running_sum_last4_sum_row_seq,
        ROW_NUMBER() OVER (ORDER BY last4_avg_row_seq, id) AS row_seq,
        NTILE(10) OVER (ORDER BY last4_max_row_seq, id) AS last4_max_row_seq_bucket
    FROM scored
)
SELECT
    id,
    days_since_last_restock,
    running_count_last4_sum_row_seq,
    running_min_last4_sum_row_seq,
    prev_running_sum_last4_sum_row_seq,
    last4_max_row_seq_bucket
FROM scored_inventory
ORDER BY prev_running_sum_last4_sum_row_seq
