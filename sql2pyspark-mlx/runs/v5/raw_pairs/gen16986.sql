WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        SUBSTRING(carrier, 1, 4) AS carrier_prefix,
        DATE_TRUNC('month', delivered_date) AS delivered_date_period
    FROM shipments
    WHERE carrier IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN delivered_date_period - LAG(delivered_date_period) OVER (PARTITION BY status ORDER BY delivered_date_period, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessions_shipments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        delivered_date,
        status,
        delivered_date_period,
        SUM(new_group_flag) OVER (PARTITION BY status ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        burst_id,
        SUM(burst_id) OVER w_hist AS last4_sum_burst_id,
        AVG(burst_id) OVER w_hist AS last4_avg_burst_id,
        MAX(burst_id) OVER w_hist AS last4_max_burst_id,
        AVG(burst_id) OVER (PARTITION BY status ORDER BY delivered_date_period, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS burst_id_avg_6_prior
    FROM sessions_shipments
    WINDOW w_hist AS (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last4_sum_burst_id,
        last4_avg_burst_id,
        burst_id_avg_6_prior,
        LEAD(burst_id, 1) OVER (ORDER BY last4_max_burst_id, id) AS next_burst_id,
        LEAD(last4_max_burst_id, 1) OVER (ORDER BY burst_id, id) AS next_last4_max_burst_id
    FROM with_windows
    WHERE last4_max_burst_id >= 3.5
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        last4_sum_burst_id,
        last4_avg_burst_id,
        burst_id_avg_6_prior,
        next_burst_id,
        next_last4_max_burst_id,
        AVG(burst_id_avg_6_prior) OVER (ORDER BY last4_avg_burst_id, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS burst_id_avg_6_prior_avg_5,
        LAST_VALUE(burst_id_avg_6_prior, TRUE) OVER w_running AS last_known_burst_id_avg_6_prior,
        LAST_VALUE(next_burst_id) IGNORE NULLS OVER (ORDER BY last4_sum_burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_burst_id
    FROM enriched
    WINDOW w_running AS (ORDER BY last4_avg_burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        last4_avg_burst_id,
        next_burst_id,
        next_last4_max_burst_id,
        NTILE(10) OVER w_running AS next_burst_id_bucket,
        AVG(next_last4_max_burst_id) OVER w_part AS next_last4_max_burst_id_avg_5
    FROM metrics
    WINDOW w_running AS (ORDER BY next_burst_id DESC, id), w_part AS (ORDER BY next_burst_id, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        last4_avg_burst_id,
        next_burst_id,
        next_last4_max_burst_id,
        next_last4_max_burst_id_avg_5,
        COALESCE(next_burst_id_bucket, 0) AS next_burst_id_bucket_filled,
        COALESCE(next_burst_id, 0) AS next_burst_id_filled
    FROM with_windows_shipments
    WHERE next_burst_id_bucket < 6
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS next_burst_id_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids_2,
    ROUND(AVG(next_burst_id_filled), 2) AS avg_next_burst_id_filled,
    ROUND(AVG(next_burst_id_filled), 2) AS avg_next_burst_id_filled_2
FROM base
GROUP BY CASE WHEN next_burst_id >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) > 1
ORDER BY avg_next_burst_id_filled_2 DESC NULLS LAST;
