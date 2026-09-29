WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        country,
        TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE browser IS DISTINCT FROM 'Chrome'
      AND browser IN ('Firefox', 'Safari')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at_date DESC NULLS FIRST, id ASC) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.event_type,
        latest.duration_sec
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.event_type, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time >= s.started_at_date
        )
),
deduped_sessions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at_date DESC, id ASC) AS rank_in_group
    FROM eligible
    QUALIFY rank_in_group = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM deduped_sessions s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at_date >= sub.start_date
       AND s.started_at_date < sub.end_date + INTERVAL 14 DAYS
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM attributed s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at_date
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        device,
        event_type,
        duration_sec
    FROM kept
) src
PIVOT (
    MAX(duration_sec) AS amount,
    COUNT(1) AS item_count
    FOR event_type IN ('purchase', 'add_to_cart')
);
