WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        DATE_TRUNC('year', started_at) AS started_at_period,
        COALESCE(browser, 'N/A') AS resolved_browser
    FROM sessions
    WHERE country IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < s.started_at
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY started_at_period DESC, id) = 1
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, b.city
    FROM latest e
    JOIN customers b
        ON e.customer_id = b.id
),
enriched_sessions AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.country AS customer_country,
        p.name
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            customer_id,
            device,
            session_start_flag
        FROM enriched_sessions
    ) src
    PIVOT (
        MAX(session_start_flag) AS value_sum,
        COUNT(1) AS n
        FOR device IN ('tablet', 'mobile', 'desktop')
    )
)
SELECT
    customer_id,
    tablet_value_sum,
    tablet_n,
    mobile_value_sum,
    desktop_value_sum,
    desktop_n
FROM wide
ORDER BY desktop_n;
