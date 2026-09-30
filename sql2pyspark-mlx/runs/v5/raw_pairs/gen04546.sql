WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(carrier, 'N/A') carrier_filled,
        UPPER(carrier) carrier_upper,
        COALESCE(carrier, 'N/A') carrier_filled_2
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-07-17')
      AND shipped_date < '2023-03-28'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_filled_2,
        LAG(cost, 1) OVER (PARTITION BY carrier, order_id ORDER BY delivered_date, id) prev_cost,
        NTILE(4) OVER (ORDER BY cost DESC, id) cost_bucket,
        SUM(cost) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_cost,
        AVG(cost) OVER (PARTITION BY carrier, order_id ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) cost_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date, id) seq_no
    FROM base
    WHERE MONTH(delivered_date) IN (3, 8, 11)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY status ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_cost,
        NTILE(3) OVER (PARTITION BY status ORDER BY cost_bucket DESC, id) cost_bucket_bucket,
        DENSE_RANK() OVER w_recent seq_no_dense_rank
    FROM scored
    WINDOW w_recent AS (PARTITION BY status ORDER BY seq_no DESC NULLS LAST)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        delivered_date,
        carrier_filled_2,
        prev_cost,
        cost_bucket,
        cumulative_cost,
        cost_avg_3_prior,
        seq_no,
        first_known_cost,
        cost_bucket_bucket,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY cost_avg_3_prior DESC NULLS LAST) cost_avg_3_prior_dense_rank,
        MAX(seq_no_dense_rank) OVER (PARTITION BY status) status_max_seq_no_dense_rank
    FROM enriched
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(seq_no) OVER w_recent running_count_seq_no,
        AVG(seq_no) OVER w_recent running_avg_seq_no,
        MIN(seq_no) OVER w_recent running_min_seq_no
    FROM with_windows
    WINDOW w_recent AS (ORDER BY cost_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base_shipments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        delivered_date,
        carrier_filled_2,
        prev_cost,
        cost_bucket,
        cumulative_cost,
        cost_avg_3_prior,
        cost_bucket_bucket,
        cost_avg_3_prior_dense_rank,
        status_max_seq_no_dense_rank,
        running_count_seq_no,
        running_avg_seq_no,
        running_min_seq_no,
        CAST(status_max_seq_no_dense_rank AS DOUBLE) status_max_seq_no_dense_rank_num
    FROM metrics
    WHERE running_count_seq_no != 6
)
-- Ranking and gap features
SELECT
    id,
    delivered_date,
    prev_cost,
    cost_bucket,
    cost_avg_3_prior,
    running_avg_seq_no,
    SUM(cost_bucket) OVER (ORDER BY cost_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_cost_bucket,
    ROW_NUMBER() OVER (ORDER BY cost_bucket DESC, id) row_seq
FROM base_shipments;
