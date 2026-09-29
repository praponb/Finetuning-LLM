WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM sessions e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.started_at BETWEEN sub.start_date AND sub.end_date
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_window
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        subscription_id,
        plan,
        monthly_fee,
        SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id, subscription_id) AS row_num
    FROM sessionized
    QUALIFY row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        browser,
        started_at,
        subscription_id,
        plan,
        monthly_fee,
        session_id,
        MAX(subscription_id) OVER (PARTITION BY customer_id ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS subscription_id_max_7d,
        LEAD(subscription_id, 2) OVER (PARTITION BY customer_id ORDER BY session_id, id, subscription_id) AS next_subscription_id,
        LEAD(monthly_fee, 1) OVER (PARTITION BY country ORDER BY row_num, id, subscription_id) AS next_monthly_fee,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_percent_rank,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY browser, country ORDER BY session_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser
    FROM latest
    WHERE subscription_id >= 5
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        s.session_id,
        s.next_monthly_fee,
        x.page,
        x.event_type
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.page, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(plan) AS plan_length,
        COALESCE(browser, 'OTHER') AS browser_filled
    FROM latest_match
    WHERE page IS NOT NULL
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, browser
    FROM cleaned
    INTERSECT
    SELECT id, browser
    FROM sessions
    WHERE country NOT IN ('JP', 'TH', 'DE')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.browser = k.browser
)
SELECT
    id,
    browser,
    subscription_id,
    plan,
    monthly_fee,
    session_id,
    page,
    event_type,
    plan_length,
    CASE WHEN plan RLIKE '^(basic|premium)$' THEN 'OTHER_ZONE' WHEN plan RLIKE '^(standard|enterprise)$' THEN 'CORE' ELSE 'GROUP_A' END AS plan_zone
FROM verified
ORDER BY page NULLS LAST;
