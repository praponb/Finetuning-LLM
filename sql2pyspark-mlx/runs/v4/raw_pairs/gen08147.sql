WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        CAST(started_at AS DATE) AS started_at_day,
        UPPER(device) AS device_upper,
        COALESCE(browser, 'OTHER') AS browser_clean
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-12-14 07:00:00')
      AND country IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        browser,
        country,
        started_at_day,
        device_upper,
        browser_clean,
        LAST_VALUE(country, TRUE) OVER w_hist AS last_known_country,
        ROW_NUMBER() OVER (ORDER BY started_at_day, id) AS seq_no,
        LAST_VALUE(browser) IGNORE NULLS OVER w_hist AS last_known_browser,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM base
    WINDOW w_hist AS (PARTITION BY country ORDER BY started_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY seq_no, id) AS row_num
        FROM with_windows
    ) x
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    DATE_TRUNC('QUARTER', started_at_day) AS quarter_start,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    SUM(CASE WHEN browser = 'Edge' THEN seq_no ELSE 0 END) AS edge_seq_no,
    SUM(seq_no) FILTER (WHERE country = 'DE') AS de_total_seq_no,
    PERCENTILE_APPROX(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    SUM(seq_no) FILTER (WHERE device = 'mobile') AS mobile_total_seq_no,
    ROUND(AVG(seq_no), 2) AS avg_seq_no
FROM ranked
GROUP BY CUBE (CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', started_at_day))
HAVING SUM(seq_no) FILTER (WHERE device = 'mobile') >= 0
ORDER BY edge_seq_no;
