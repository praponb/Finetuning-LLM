WITH valid AS (
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        INITCAP(TRIM(event_type)) AS event_type_title,
        COALESCE(page, 'N/A') AS resolved_page
    FROM web_events
    WHERE event_time > DATE '2023-08-23'
      AND page IS NOT NULL
      AND page IS NOT NULL
),
enriched AS (
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        event_type_title,
        resolved_page,
        LEAD(duration_sec, 2) OVER (ORDER BY duration_sec, id) AS next_duration_sec,
        NTILE(4) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        MAX(duration_sec) OVER (PARTITION BY session_id) AS session_id_max_duration_sec
    FROM valid
),
enriched_web_events AS (
    SELECT a.*, p.browser
    FROM enriched a
    LEFT JOIN sessions p
        ON a.session_id = p.id
)
SELECT
    browser,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    APPROX_PERCENTILE(duration_sec, 0.9) AS duration_sec_p90
FROM enriched_web_events
GROUP BY ROLLUP (browser)
ORDER BY browser NULLS FIRST
