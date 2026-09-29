WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS latest_rank
        FROM sessions
    ) t
    WHERE latest_rank = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at <= DATE_ADD(sub.end_date, 3)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_window
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM sessions_sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < s.started_at
        )
)
-- Join a dimension and aggregate
SELECT
    dim.country AS customer_country,
    src.plan,
    COUNT(*) AS num_rows,
    MIN(src.session_start_flag) AS min_session_start_flag
FROM eligible src
INNER JOIN customers dim ON src.customer_id = dim.id
GROUP BY CUBE (dim.country, src.plan)
