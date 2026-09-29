WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(country, TRUE) OVER w_part AS first_known_country,
        ROW_NUMBER() OVER (PARTITION BY customer_id, country ORDER BY started_at, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS event_seq,
        LAST_VALUE(browser, TRUE) OVER w_part AS last_known_browser,
        LAST_VALUE(country) IGNORE NULLS OVER w_part_2 AS last_known_country
    FROM sessions
    WINDOW w_part AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part_2 AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        row_seq - event_seq AS row_seq_minus_event_seq,
        UPPER(country) AS country_upper,
        COALESCE(browser, 'NONE') AS resolved_browser
    FROM latest
    WHERE browser IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM kept e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at >= sub.start_date
       AND e.started_at < sub.end_date + INTERVAL 7 DAYS
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    subscription_id,
    browser,
    metric,
    reading
FROM (
    SELECT
        id,
        subscription_id,
        browser,
        row_seq_minus_event_seq,
        monthly_fee
    FROM attributed
) src
UNPIVOT (
    reading FOR metric IN (row_seq_minus_event_seq AS ROW_SEQ_MINUS_EVENT_SEQ, monthly_fee AS MONTHLY_FEE_VAL)
) u
