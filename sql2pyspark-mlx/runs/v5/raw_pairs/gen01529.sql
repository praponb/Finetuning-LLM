WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        LENGTH(status) AS status_length,
        TRIM(carrier) AS clean_carrier
    FROM shipments
    WHERE carrier IS NOT NULL
      AND cost >= 69
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY status_length, id) AS status_length_bucket,
        ROW_NUMBER() OVER (ORDER BY delivered_date, id) AS row_seq,
        COUNT(status_length) OVER (PARTITION BY status) AS status_count_status_length
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        status_length,
        clean_carrier,
        status_length_bucket,
        row_seq,
        status_count_status_length,
        MIN(row_seq) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_row_seq,
        MAX(row_seq) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_row_seq,
        SUM(row_seq) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_row_seq,
        COUNT(row_seq) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_row_seq,
        ROUND(status_count_status_length / SUM(status_count_status_length) OVER (PARTITION BY status, order_id), 4) AS status_count_status_length_share
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(row_seq) OVER w_running AS row_seq_avg_2,
        LAST_VALUE(status_length, TRUE) OVER w_hist AS last_known_status_length,
        LEAD(running_count_row_seq, 2) OVER (PARTITION BY order_id ORDER BY running_count_row_seq, id) AS next_running_count_row_seq
    FROM enriched
    WINDOW w_running AS (PARTITION BY order_id ORDER BY status_count_status_length_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY order_id ORDER BY status_count_status_length_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
        CASE WHEN running_count_row_seq >= 16 THEN 'HIGH' ELSE 'LOW' END AS running_count_row_seq_tier,
        ROUND(AVG(status_count_status_length_share), 2) AS avg_status_count_status_length_share,
        ROUND(AVG(row_seq), 2) AS avg_row_seq,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(running_max_row_seq, ARRAY(0.25, 0.5, 0.75)) AS running_max_row_seq_quartiles,
        GROUPING_ID() AS grouping_level
    FROM with_windows
    GROUP BY CUBE (DATE_TRUNC('QUARTER', delivered_date), CASE WHEN running_count_row_seq >= 16 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    quarter_start,
    running_count_row_seq_tier,
    avg_status_count_status_length_share,
    running_max_row_seq_quartiles,
    grouping_level
FROM stats
ORDER BY quarter_start DESC NULLS FIRST;
