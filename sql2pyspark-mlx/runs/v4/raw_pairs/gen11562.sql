WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        CASE WHEN device = 'mobile' THEN 1 ELSE 0 END AS is_mobile
    FROM sessions
    WHERE started_at >= TO_DATE('2023-12-27')
      AND browser IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    browser,
    MAX(is_mobile) AS max_is_mobile,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    COUNT(*) AS num_rows
FROM sessionized
GROUP BY browser
ORDER BY mobile_count;
