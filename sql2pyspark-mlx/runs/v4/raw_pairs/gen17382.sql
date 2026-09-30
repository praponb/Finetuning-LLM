WITH valid AS (
    SELECT
        *,
        WEEKOFYEAR(started_at) AS started_at_weekofyear,
        COALESCE(browser, 'DIRECT') AS resolved_browser
    FROM sessions
    WHERE started_at >= DATE '2024-02-26'
      AND country IS NOT NULL
      AND device IN ('tablet', 'desktop')
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at_weekofyear, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
metrics AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        started_at_weekofyear,
        resolved_browser,
        rank_in_group,
        NTILE(4) OVER (PARTITION BY device ORDER BY started_at_weekofyear, id) AS started_at_weekofyear_bucket,
        LAST_VALUE(browser, TRUE) OVER (ORDER BY started_at_weekofyear, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (ORDER BY rank_in_group DESC, id) AS row_seq,
        LAG(started_at_weekofyear, 1) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at_weekofyear,
        NTILE(5) OVER (PARTITION BY device ORDER BY started_at_weekofyear, id) AS started_at_weekofyear_bucket_2
    FROM latest
),
filtered AS (
    SELECT
        *,
        CONCAT(device, ', ', country) AS device_country,
        COALESCE(browser, 'NONE') AS resolved_browser_2
    FROM metrics
    WHERE started_at > TIMESTAMP '2023-07-16 21:00:00'
),
prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM filtered
),
gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_sessions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.country,
    s.started_at_weekofyear,
    s.resolved_browser,
    s.rank_in_group,
    s.started_at_weekofyear_bucket,
    s.prev_started_at_weekofyear,
    CASE WHEN browser RLIKE '^(Safari|Edge)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS browser_zone,
    (SELECT MIN(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM sessions_sessions s
ORDER BY customer_id DESC NULLS LAST;
