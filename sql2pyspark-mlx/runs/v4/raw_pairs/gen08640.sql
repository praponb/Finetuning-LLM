WITH enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, browser ORDER BY started_at, id) AS seq_no,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser
    FROM sessions
)
SELECT
    country,
    CASE WHEN seq_no >= 6 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
    PERCENTILE_APPROX(seq_no, 0.5) AS seq_no_p50,
    COUNT(*) AS row_count,
    MAX(seq_no) AS max_seq_no,
    COUNT_IF(device = 'desktop') AS desktop_events,
    SIZE(COLLECT_SET(device)) AS num_devices,
    RANK() OVER (PARTITION BY country ORDER BY MAX(seq_no) DESC) AS max_seq_no_rank
FROM enriched
GROUP BY country, CASE WHEN seq_no >= 6 THEN 'LARGE' ELSE 'SMALL' END
HAVING desktop_events < 1
ORDER BY desktop_events DESC NULLS FIRST
