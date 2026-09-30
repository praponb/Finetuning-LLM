WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY started_at, id) AS seq_no
    FROM sessions
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', started_at) AS year_start,
    COUNT_IF(device = 'desktop') AS desktop_events,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    SIZE(COLLECT_SET(country)) AS num_countries,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    SUM(CASE WHEN browser = 'Chrome' THEN seq_no ELSE 0 END) AS chrome_seq_no,
    COUNT(*) AS cnt
FROM scored
GROUP BY year_start
ORDER BY num_countries DESC NULLS LAST;
