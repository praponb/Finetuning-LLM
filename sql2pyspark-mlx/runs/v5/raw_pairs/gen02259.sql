WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        paid_at,
        status,
        MAX(amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d,
        LAST_VALUE(method, TRUE) OVER (PARTITION BY order_id, status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method,
        LAST_VALUE(method, TRUE) OVER (PARTITION BY method, order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method_2
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 9 AND 17
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM metrics
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessions_payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        method,
        paid_at,
        last_known_method,
        last_known_method_2,
        new_group_flag,
        SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        last_known_method_2,
        new_group_flag,
        burst_id,
        LAG(burst_id, 2) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_burst_id,
        SUM(new_group_flag) OVER w_part AS new_group_flag_sum_30d,
        NTILE(4) OVER (PARTITION BY method ORDER BY burst_id DESC, id) AS burst_id_bucket
    FROM sessionized
    WINDOW w_part AS (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN new_group_flag >= 1 THEN 'HIGH' ELSE 'LOW' END AS new_group_flag_tier,
        CASE WHEN prev_burst_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_burst_id_tier,
        COUNT(*) AS event_count,
        COUNT(*) AS row_count
    FROM enriched
    GROUP BY new_group_flag_tier, prev_burst_id_tier
)
SELECT
    new_group_flag_tier,
    prev_burst_id_tier,
    event_count,
    row_count
FROM daily
ORDER BY prev_burst_id_tier DESC NULLS LAST, new_group_flag_tier
LIMIT 10;
