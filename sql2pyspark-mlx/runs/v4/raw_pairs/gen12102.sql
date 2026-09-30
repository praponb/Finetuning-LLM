WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        TRIM(country) AS clean_country,
        SUBSTRING(device, 1, 2) AS device_prefix
    FROM sessions
    WHERE started_at >= DATE '2024-04-13'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.country AS customer_country,
        c.signup_date
    FROM valid a
    LEFT JOIN customers c
        ON a.customer_id = c.id
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.web_event_page,
        latest.web_event_duration_sec
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page, c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.signup_date
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, b.city
    FROM with_last src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, device ORDER BY signup_date, id) AS rn
        FROM joined
    ) t
    WHERE rn = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY city ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY city ORDER BY started_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM deduped
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY city ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.country,
    s.device_prefix,
    s.customer_country,
    s.web_event_page,
    s.web_event_duration_sec,
    (SELECT AVG(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM islands s
ORDER BY customer_id
