WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(browser) AS browser_lower
    FROM sessions
    WHERE started_at > TO_DATE('2023-11-26')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) AS prev_started_at
    FROM valid
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        browser_lower,
        prev_started_at,
        new_group_flag,
        session_no,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY new_group_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        SUM(new_group_flag) OVER (PARTITION BY browser ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS new_group_flag_sum_7d
    FROM islands
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device, browser ORDER BY started_at DESC, id) = 1
)
SELECT
    id,
    customer_id,
    device,
    started_at,
    country,
    browser_lower,
    prev_started_at,
    session_no,
    last_known_country
FROM latest;
