WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        end_date,
        status,
        DATE_ADD(start_date, 16) AS start_date_plus_16d
    FROM subscriptions
    WHERE end_date > DATE '2024-03-23'
      AND plan NOT IN ('standard', 'premium')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        end_date,
        status,
        start_date_plus_16d,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY end_date, id) AS seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date_plus_16d DESC, id) AS row_seq_2,
        ROW_NUMBER() OVER (ORDER BY start_date_plus_16d DESC, id) AS event_seq
    FROM cleaned
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(row_seq_2) OVER (PARTITION BY status ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq_2,
        AVG(event_seq) OVER (ORDER BY row_seq_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_avg_3,
        SUM(row_seq_2) OVER (PARTITION BY status, customer_id) AS status_sum_row_seq_2,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY customer_id), 4) AS event_seq_share
    FROM scored
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date_plus_16d NULLS LAST, id) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM latest
    EXCEPT ALL
    SELECT id, status
    FROM subscriptions
    WHERE plan = 'enterprise'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id AND s.status = k.status
),
scored_subscriptions AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(row_seq) OVER (PARTITION BY status) AS status_stddev_samp_row_seq,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY event_seq_avg_3 DESC) AS event_seq_avg_3_percent_rank,
        CUME_DIST() OVER (PARTITION BY status ORDER BY event_seq) AS event_seq_cume_dist
    FROM kept_rows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN event_seq_share >= 0.66 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_share_tier,
    DATE_TRUNC('QUARTER', end_date) AS quarter_start,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING_ID() AS grouping_level
FROM scored_subscriptions
GROUP BY CUBE (CASE WHEN event_seq_share >= 0.66 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', end_date))
HAVING distinct_ids > 1
ORDER BY num_status DESC;
