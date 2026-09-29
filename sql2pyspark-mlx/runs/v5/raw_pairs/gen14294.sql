WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        HOUR(started_at) AS started_at_hour,
        CONCAT_WS('-', device, browser) AS device_browser_key,
        COALESCE(country, 'OTHER') AS country_filled
    FROM sessions
    WHERE browser = 'Firefox'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device,
        browser,
        country,
        device_browser_key,
        SUM(started_at_hour) OVER (PARTITION BY device ORDER BY started_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_started_at_hour,
        AVG(started_at_hour) OVER (ORDER BY started_at, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS started_at_hour_avg_5_prior,
        AVG(started_at_hour) OVER (PARTITION BY device) AS device_avg_started_at_hour,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY country ORDER BY started_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country
    FROM cleaned
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(started_at_hour_avg_5_prior) OVER (PARTITION BY country ORDER BY device_avg_started_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_started_at_hour_avg_5_prior,
        LEAD(cumulative_started_at_hour, 1) OVER (PARTITION BY browser, device ORDER BY started_at_hour_avg_5_prior, id) AS next_cumulative_started_at_hour,
        SUM(started_at_hour_avg_5_prior) OVER (PARTITION BY browser ORDER BY started_at_hour_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_started_at_hour_avg_5_prior_2
    FROM with_windows
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.duration_sec < 279
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        browser,
        country,
        device_browser_key,
        cumulative_started_at_hour,
        last_known_country,
        running_started_at_hour_avg_5_prior,
        running_started_at_hour_avg_5_prior_2,
        COALESCE(device_avg_started_at_hour, 0) AS device_avg_started_at_hour_filled,
        COALESCE(country, 'NONE') AS country_filled
    FROM eligible
    WHERE device_avg_started_at_hour BETWEEN 28 AND 30
)
-- Grouped metrics
SELECT
    browser,
    COUNT(DISTINCT browser) AS distinct_browsers,
    ROUND(AVG(running_started_at_hour_avg_5_prior), 2) AS avg_running_started_at_hour_avg_5_prior,
    ROUND(AVG(cumulative_started_at_hour), 2) AS avg_cumulative_started_at_hour,
    COUNT_IF(browser = 'Edge') AS edge_events
FROM valid
GROUP BY browser;
