WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        INITCAP(TRIM(country)) AS country_title
    FROM sessions
    WHERE browser <> 'Safari'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY started_at, id) AS row_seq_2
    FROM valid
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        country_title,
        row_seq,
        row_seq_2,
        COUNT(row_seq_2) OVER (PARTITION BY customer_id, browser ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS row_seq_2_count_7d,
        SUM(row_seq_2) OVER (PARTITION BY customer_id ORDER BY row_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq_2,
        LEAD(row_seq, 1, 0) OVER (PARTITION BY country ORDER BY row_seq, id) AS next_row_seq
    FROM scored
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', started_at) AS year_start,
        SIZE(COLLECT_SET(browser)) AS num_browsers,
        COUNT(DISTINCT browser) AS distinct_browsers,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        APPROX_PERCENTILE(row_seq_2_count_7d, 0.9) AS row_seq_2_count_7d_p90,
        COUNT_IF(browser = 'Chrome') AS chrome_events,
        SORT_ARRAY(COLLECT_SET(browser)) AS browser_list
    FROM not_flagged
    GROUP BY DATE_TRUNC('YEAR', started_at)
)
SELECT
    year_start,
    num_browsers,
    distinct_browsers,
    row_seq_2_count_7d_p90,
    chrome_events,
    browser_list
FROM stats
