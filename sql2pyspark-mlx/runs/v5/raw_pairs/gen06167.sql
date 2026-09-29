WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status,
        DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date,
        DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date
    FROM shipments
    WHERE order_id IS NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        delivered_date,
        cost,
        days_since_delivered_date,
        STDDEV_SAMP(days_shipped_date_to_delivered_date) OVER (PARTITION BY carrier) AS carrier_stddev_samp_days_shipped_date_to_delivered_date,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share,
        LEAD(days_since_delivered_date, 2, 0) OVER (ORDER BY days_since_delivered_date, id) AS next_days_since_delivered_date,
        AVG(days_since_delivered_date) OVER (PARTITION BY carrier ORDER BY days_since_delivered_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS days_since_delivered_date_avg_6_prior,
        RANK() OVER (PARTITION BY carrier, status ORDER BY days_since_delivered_date DESC NULLS LAST) AS days_since_delivered_date_rank
    FROM base
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY days_since_delivered_date_avg_6_prior DESC, id) AS days_since_delivered_date_avg_6_prior_bucket,
        AVG(days_since_delivered_date) OVER (ORDER BY days_since_delivered_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS days_since_delivered_date_avg_3,
        ROW_NUMBER() OVER (ORDER BY delivered_date, id) AS row_seq,
        LAG(cost_share, 1) OVER (ORDER BY delivered_date, id) AS prev_cost_share
    FROM metrics
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        delivered_date,
        next_days_since_delivered_date,
        days_since_delivered_date_avg_6_prior,
        days_since_delivered_date_avg_6_prior_bucket,
        days_since_delivered_date_avg_3,
        row_seq,
        prev_cost_share,
        LAG(carrier_stddev_samp_days_shipped_date_to_delivered_date, 1) OVER w_recent AS prev_carrier_stddev_samp_days_shipped_date_to_delivered_date,
        FIRST_VALUE(cost, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        RANK() OVER (ORDER BY carrier_stddev_samp_days_shipped_date_to_delivered_date DESC) AS carrier_stddev_samp_days_shipped_date_to_delivered_date_rank
    FROM scored
    WINDOW w_recent AS (ORDER BY days_since_delivered_date_rank, id)
),
metrics_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(carrier_stddev_samp_days_shipped_date_to_delivered_date_rank) OVER w_grp AS prev_carrier_stddev_samp_days_shipped_date_to_delivered_date_rank,
        LAG(prev_carrier_stddev_samp_days_shipped_date_to_delivered_date) OVER w_grp AS prev_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date,
        LEAD(prev_carrier_stddev_samp_days_shipped_date_to_delivered_date) OVER w_grp AS next_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM with_windows
    WINDOW w_grp AS (ORDER BY days_since_delivered_date_avg_6_prior_bucket, id)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(days_since_delivered_date_avg_6_prior, ARRAY(0.25, 0.5, 0.75)) AS days_since_delivered_date_avg_6_prior_quartiles,
    ROUND(AVG(prev_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date), 2) AS avg_prev_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date
FROM metrics_shipments
GROUP BY DATE_TRUNC('QUARTER', delivered_date)
HAVING avg_prev_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date < 1000000
