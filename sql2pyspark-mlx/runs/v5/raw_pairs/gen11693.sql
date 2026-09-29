WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        CASE WHEN method IN ('promptpay', 'cash') THEN 'Group A' ELSE 'Other' END AS method_group
    FROM payments
    WHERE method IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        method_group,
        SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) AS amount_share,
        AVG(island_id) OVER (PARTITION BY method, status) AS method_avg_island_id,
        LAST_VALUE(method) IGNORE NULLS OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method,
        SUM(island_id) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_island_id
    FROM sessionized
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY method, status ORDER BY running_island_id DESC NULLS LAST) AS dedup_rank
    FROM scored
    QUALIFY dedup_rank = 1
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM deduped
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps_2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(method_avg_island_id) OVER w_part AS last3_max_method_avg_island_id,
        SUM(method_avg_island_id) OVER w_part AS last3_sum_method_avg_island_id,
        AVG(method_avg_island_id) OVER w_part AS last3_avg_method_avg_island_id
    FROM sessions_payments
    WINDOW w_part AS (PARTITION BY method ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM metrics
),
sessions_payments_2 AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        amount,
        paid_at,
        island_id,
        method_avg_island_id,
        is_new_session,
        session_no,
        last3_max_method_avg_island_id,
        last3_sum_method_avg_island_id,
        last3_avg_method_avg_island_id,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
daily AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', paid_at) AS year_start,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN method = 'cash' THEN last3_sum_method_avg_island_id ELSE 0 END) AS cash_last3_sum_method_avg_island_id,
        SUM(amount) FILTER (WHERE method = 'paypal') AS paypal_total_amount,
        GROUPING_ID() AS gid
    FROM sessions_payments_2
    GROUP BY ROLLUP (DATE_TRUNC('YEAR', paid_at))
)
SELECT
    year_start,
    cash_last3_sum_method_avg_island_id,
    gid
FROM daily
ORDER BY year_start DESC
