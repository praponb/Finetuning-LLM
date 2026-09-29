WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.event_type,
        x.web_event_page
    FROM sessions s
    JOIN LATERAL (
            SELECT c.event_type, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest_match e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at >= sub.start_date
       AND e.started_at <= DATE_ADD(sub.end_date, 7)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.email,
        c.country AS customer_country,
        c.age
    FROM matched e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
enriched_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY plan ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        AVG(age) OVER (PARTITION BY browser, customer_country) AS browser_avg_age
    FROM enriched
),
remaining_keys AS (
    -- Set difference on keys
    SELECT browser
    FROM enriched_sessions
    EXCEPT
    SELECT browser
    FROM sessions
    WHERE customer_id IN (12, 27, 37)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_sessions s
    INNER JOIN remaining_keys k ON s.browser = k.browser
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    COUNT_IF(country = 'JP') AS jp_events
FROM verified
GROUP BY DATE_TRUNC('QUARTER', started_at)
ORDER BY jp_events NULLS FIRST;
