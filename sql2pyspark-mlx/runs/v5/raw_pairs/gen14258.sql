WITH filtered AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        LENGTH(country) AS country_length,
        UPPER(browser) AS browser_upper
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-06-16 20:00:00'
      AND country IS DISTINCT FROM 'UK'
),
with_last AS (
    SELECT
        s.*,
        top1.page,
        top1.event_time
    FROM filtered s
    JOIN LATERAL (
            SELECT c.page, c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.session_id IN (19, 37)
        )
),
gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY customer_id ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM not_flagged
),
sessions_sessions AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
joined AS (
    SELECT
        s.*,
        p.email,
        p.segment,
        p.country AS customer_country
    FROM sessions_sessions s
    INNER JOIN customers p
        ON s.customer_id = p.id
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country_length,
    s.browser_upper,
    s.page,
    s.event_time,
    CASE WHEN browser RLIKE '^(Safari|Chrome)$' THEN 'EXTENDED' WHEN browser RLIKE '^(Firefox|Edge)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS browser_zone,
    (SELECT MIN(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM joined s
ORDER BY browser_zone NULLS LAST;
