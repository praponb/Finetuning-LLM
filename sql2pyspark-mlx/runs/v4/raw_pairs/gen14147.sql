WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY duration_sec DESC, id ASC) AS latest_rank
    FROM web_events
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id ASC) AS seq_no,
        NTILE(10) OVER (PARTITION BY page ORDER BY duration_sec DESC, id ASC) AS duration_sec_bucket,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY latest_rank ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_avg_6,
        NTILE(4) OVER w_running AS duration_sec_bucket_2
    FROM latest
    WINDOW w_running AS (PARTITION BY event_type ORDER BY duration_sec DESC, id ASC)
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        s.browser,
        s.device,
        s.country
    FROM with_windows src
    LEFT JOIN sessions s
        ON src.session_id = s.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        page,
        CASE WHEN duration_sec_bucket_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_bucket_2_tier,
        DATE_TRUNC('WEEK', event_time) AS week_start,
        SUM(CASE WHEN device = 'tablet' THEN duration_sec ELSE 0 END) AS tablet_duration_sec,
        ROUND(SUM(duration_sec_avg_6) / NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0), 2) AS duration_sec_avg_6_per_page_view
    FROM enriched
    GROUP BY page, CASE WHEN duration_sec_bucket_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', event_time)
    HAVING ROUND(SUM(duration_sec_avg_6) / NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0), 2) >= 0
)
SELECT
    page,
    duration_sec_bucket_2_tier,
    week_start,
    duration_sec_avg_6_per_page_view
FROM summary
ORDER BY week_start ASC NULLS FIRST, duration_sec_bucket_2_tier DESC
