WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec IS NOT NULL
        )
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM kept src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at >= sub.start_date
       AND src.started_at <= DATE_ADD(sub.end_date, 3)
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.event_type,
        latest.web_event_duration_sec
    FROM attributed s
    JOIN LATERAL (
            SELECT c.event_type, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(web_event_duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS web_event_duration_sec_sum_30d,
        SUM(monthly_fee) OVER (PARTITION BY event_type) AS event_type_sum_monthly_fee,
        SUM(web_event_duration_sec) OVER (PARTITION BY event_type, plan ORDER BY monthly_fee, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_web_event_duration_sec
    FROM with_latest
    WHERE device IN ('tablet', 'mobile', 'desktop')
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    subscription_id,
    plan,
    monthly_fee
FROM scored
ORDER BY id NULLS LAST
