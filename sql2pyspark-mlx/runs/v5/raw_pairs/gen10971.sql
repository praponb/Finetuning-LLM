WITH with_windows AS (
    -- Running and moving aggregates
    SELECT *, LAST_VALUE(browser, TRUE) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser
    FROM sessions
    WHERE country = 'TH'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        browser,
        started_at,
        country,
        CONCAT(device, '/', browser) AS device_browser,
        COALESCE(country, 'N/A') AS country_filled
    FROM with_windows
    WHERE country IS NOT DISTINCT FROM 'US'
      AND started_at > DATE '2023-04-05'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, browser ORDER BY started_at DESC NULLS FIRST, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        browser,
        started_at,
        country,
        device_browser,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS row_seq,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY country, browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        LAST_VALUE(country) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_2
    FROM ranked
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(row_seq, 2) OVER (PARTITION BY country ORDER BY row_seq, id) AS next_row_seq,
        AVG(row_seq) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS row_seq_avg_3,
        LEAD(row_seq, 2) OVER (ORDER BY row_seq, id) AS next_row_seq_2,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY country), 4) AS row_seq_share,
        SUM(row_seq) OVER (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS row_seq_sum_90d
    FROM metrics
)
-- Grouped metrics
SELECT
    country,
    CASE WHEN next_row_seq >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_row_seq_tier,
    MIN(started_at) AS first_seen,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    MAX(row_seq_share) AS max_row_seq_share
FROM enriched
GROUP BY country, next_row_seq_tier;
