WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM sessions s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at BETWEEN sub.start_date AND sub.end_date
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM matched e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at BETWEEN sub.start_date AND sub.end_date
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY subscription_plan ORDER BY started_at ASC, id ASC, subscription_id ASC, subscription_id_2 ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY subscription_plan ORDER BY started_at ASC, id ASC, subscription_id ASC, subscription_id_2 ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_window
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        browser,
        started_at,
        country,
        subscription_id,
        subscription_id_2,
        subscription_plan,
        subscription_monthly_fee,
        SUM(is_new_burst) OVER (PARTITION BY subscription_plan ORDER BY started_at ASC, id ASC, subscription_id ASC, subscription_id_2 ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY subscription_plan ORDER BY subscription_monthly_fee ASC, id ASC, subscription_id ASC, subscription_id_2 ASC) AS subscription_monthly_fee_bucket,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY subscription_monthly_fee ASC, id ASC, subscription_id ASC, subscription_id_2 ASC) AS event_seq,
        STDDEV_SAMP(subscription_id) OVER (PARTITION BY country) AS country_stddev_samp_subscription_id,
        MIN(subscription_monthly_fee) OVER (PARTITION BY subscription_plan) AS subscription_plan_min_subscription_monthly_fee
    FROM islands
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        s.started_at,
        s.subscription_id,
        s.subscription_id_2,
        s.subscription_plan,
        s.subscription_monthly_fee,
        s.island_id,
        s.subscription_monthly_fee_bucket,
        s.event_seq,
        x.web_event_event_time,
        x.web_event_event_type
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', web_event_event_time) AS year_start,
        SUM(island_id) FILTER (WHERE browser = 'Edge') AS edge_total_island_id,
        COUNT(CASE WHEN subscription_plan = 'basic' THEN 1 END) AS basic_count,
        COUNT_IF(browser = 'Edge') AS edge_events
    FROM latest_match
    GROUP BY DATE_TRUNC('YEAR', web_event_event_time)
)
SELECT
    year_start,
    edge_total_island_id,
    basic_count,
    edge_events
FROM daily
