WITH with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.web_event_event_type
    FROM sessions s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(web_event_event_type, TRUE) OVER w_hist AS last_known_web_event_event_type,
        LAST_VALUE(country) IGNORE NULLS OVER w_hist AS last_known_country
    FROM with_last
    WINDOW w_hist AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC NULLS FIRST, id) AS latest_rank
        FROM metrics
    ) ranked
    WHERE latest_rank = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM deduped a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at < sub.end_date + INTERVAL 30 DAYS
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id) AS prev_started_at
    FROM with_window
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM sessionized e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at >= sub.start_date
       AND e.started_at <= DATE_ADD(sub.end_date, 10)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM attributed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id, subscription_id_2) = 1
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    s.web_event_event_type,
    s.last_known_web_event_event_type,
    s.last_known_country,
    CASE WHEN browser RLIKE '^(Safari|Chrome)$' THEN 'PRIORITY' ELSE 'CORE' END AS browser_zone,
    (SELECT MAX(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM latest s
ORDER BY browser NULLS FIRST;
