WITH deduped AS (
    SELECT *
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC NULLS LAST, id ASC) = 1
),
joined AS (
    SELECT
        e.id,
        e.customer_id,
        e.browser,
        e.started_at,
        c.signup_date,
        c.country AS customer_country,
        c.segment
    FROM deduped e
    INNER JOIN customers c
        ON e.customer_id = c.id
    WHERE c.country NOT IN ('US', 'DE')
),
matched_keys AS (
    SELECT customer_id, browser
    FROM joined
    INTERSECT
    SELECT customer_id, browser
    FROM sessions
    WHERE TO_DATE(started_at) < '2024-04-01'
),
kept_rows AS (
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id AND s.browser = k.browser
),
not_flagged AS (
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
)
SELECT customer_id, signup_date
FROM not_flagged
ORDER BY signup_date DESC NULLS LAST;
