WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        country,
        HOUR(started_at) started_at_hour,
        CONCAT_WS('-', browser, device) browser_device_key,
        COALESCE(browser, 'NONE') browser_filled
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-07-05 09:00:00'
      AND browser IS NOT NULL
),
cleaned_sessions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN browser = 'Chrome' THEN 1 ELSE 0 END is_chrome,
        SUBSTRING(country, 1, 2) country_prefix,
        COALESCE(browser, 'N/A') resolved_browser
    FROM cleaned
    WHERE started_at_hour BETWEEN 53 AND 78
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        started_at_hour,
        browser_filled,
        is_chrome,
        country_prefix,
        AVG(started_at_hour) OVER (ORDER BY is_chrome, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) started_at_hour_avg_2,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY started_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY country ORDER BY is_chrome, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_browser,
        LEAD(started_at_hour, 1, 0) OVER (PARTITION BY customer_id ORDER BY is_chrome, id) next_started_at_hour,
        AVG(is_chrome) OVER (PARTITION BY country ORDER BY started_at_hour, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) is_chrome_avg_6_prior
    FROM cleaned_sessions
    WHERE started_at_hour > 73
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        browser_filled,
        is_chrome,
        first_known_country,
        first_known_browser,
        SUM(started_at_hour_avg_2) OVER (ORDER BY is_chrome, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_started_at_hour_avg_2,
        MIN(started_at_hour_avg_2) OVER (ORDER BY is_chrome, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_started_at_hour_avg_2,
        MAX(started_at_hour_avg_2) OVER (ORDER BY is_chrome, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_started_at_hour_avg_2,
        ROW_NUMBER() OVER (ORDER BY next_started_at_hour, id) event_seq
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_max_started_at_hour_avg_2 >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_max_started_at_hour_avg_2_tier,
    CASE WHEN is_chrome >= 76 THEN 'LARGE' ELSE 'SMALL' END AS is_chrome_tier,
    CASE WHEN running_min_started_at_hour_avg_2 >= 70 THEN 'LARGE' ELSE 'SMALL' END AS running_min_started_at_hour_avg_2_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(is_chrome) AS avg_is_chrome,
    ROUND(AVG(running_max_started_at_hour_avg_2), 2) AS avg_running_max_started_at_hour_avg_2,
    MIN(running_min_started_at_hour_avg_2) AS min_running_min_started_at_hour_avg_2
FROM metrics
GROUP BY ROLLUP (CASE WHEN running_max_started_at_hour_avg_2 >= 66 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN is_chrome >= 76 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_min_started_at_hour_avg_2 >= 70 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY avg_is_chrome NULLS LAST;
