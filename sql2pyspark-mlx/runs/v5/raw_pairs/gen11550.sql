WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(status, ', ', carrier) AS status_carrier
    FROM shipments
    WHERE carrier IS NOT NULL
      AND carrier IS DISTINCT FROM 'Flash'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(cost * 1.15, 2) AS cost_scaled
    FROM base
    WHERE delivered_date > DATE '2023-04-15'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost_scaled) OVER (PARTITION BY carrier ORDER BY cost_scaled, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_scaled_avg_5_prior,
        LEAD(cost, 2, 0) OVER (PARTITION BY carrier, order_id ORDER BY delivered_date, id) AS next_cost,
        SUM(cost_scaled) OVER (PARTITION BY order_id, status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_scaled,
        DENSE_RANK() OVER (PARTITION BY order_id, carrier ORDER BY cost_scaled NULLS LAST) AS cost_scaled_dense_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share
    FROM valid
),
valid_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status_carrier,
        cost_scaled,
        cost_scaled_avg_5_prior,
        running_cost_scaled,
        cost_share,
        COALESCE(cost_scaled_dense_rank, 0) AS cost_scaled_dense_rank_filled
    FROM enriched
    WHERE delivered_date > DATE '2023-03-19'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_share DESC, id) AS row_num
        FROM valid_shipments
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        shipped_date,
        cost_scaled,
        cost_share,
        cost_scaled_dense_rank_filled,
        row_num,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost NULLS FIRST) AS cost_dense_rank,
        SUM(cost_scaled) OVER (PARTITION BY carrier ORDER BY cost_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_scaled_2,
        ROW_NUMBER() OVER (ORDER BY cost_scaled_avg_5_prior, id) AS row_seq,
        SUM(cost_scaled) OVER (ORDER BY cost_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_scaled_3,
        SUM(running_cost_scaled) OVER (PARTITION BY carrier ORDER BY cost_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_cost_scaled
    FROM deduped
),
metrics_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        shipped_date,
        cost_scaled,
        cost_share,
        cost_scaled_dense_rank_filled,
        row_num,
        cost_dense_rank,
        running_cost_scaled_2,
        cumulative_running_cost_scaled,
        ROW_NUMBER() OVER (ORDER BY shipped_date DESC, id) AS row_seq_2,
        AVG(cost_share) OVER (ORDER BY cost_share, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_share_avg_5_prior,
        SUM(cost_scaled) OVER (ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_scaled,
        LAG(cost_share, 1, 0) OVER (ORDER BY cost_scaled_dense_rank_filled, id) AS prev_cost_share,
        NTILE(10) OVER w_hist AS cost_scaled_dense_rank_filled_bucket
    FROM metrics
    WINDOW w_hist AS (ORDER BY cost_scaled_dense_rank_filled DESC, id)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    CASE WHEN row_seq_2 >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_2_tier,
    PERCENTILE_APPROX(cost_scaled_dense_rank_filled_bucket, ARRAY(0.25, 0.5, 0.75)) AS cost_scaled_dense_rank_filled_bucket_quartiles,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2
FROM metrics_shipments
GROUP BY 1, 2;
