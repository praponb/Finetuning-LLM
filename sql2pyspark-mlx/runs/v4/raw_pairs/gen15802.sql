WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(device) AS clean_device
    FROM sessions
    WHERE device = 'mobile'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM valid
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY prev_started_at DESC NULLS LAST, id) AS latest_rank
        FROM sessionized
    ) ranked
    WHERE latest_rank = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM deduped e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at >= sub.start_date
       AND e.started_at <= DATE_ADD(sub.end_date, 3)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS row_seq,
        LAST_VALUE(subscription_id, TRUE) OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_subscription_id
    FROM with_window
    WINDOW w_running AS (PARTITION BY browser ORDER BY prev_started_at DESC, id, subscription_id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS event_seq,
        LEAD(row_seq, 1, 0) OVER w_running AS next_row_seq,
        AVG(session_start_flag) OVER (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS session_start_flag_avg_7d,
        AVG(subscription_id) OVER (PARTITION BY customer_id) AS customer_id_avg_subscription_id,
        NTILE(3) OVER (PARTITION BY country ORDER BY row_seq, id, subscription_id) AS row_seq_bucket
    FROM metrics
    WINDOW w_recent AS (ORDER BY started_at, id, subscription_id), w_running AS (PARTITION BY browser ORDER BY monthly_fee, id, subscription_id)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        customer_id,
        device,
        burst_id
    FROM scored
) src
PIVOT (
    AVG(burst_id) AS total,
    COUNT(1) AS n
    FOR device IN ('desktop' AS desktop, 'mobile' AS mobile)
);
