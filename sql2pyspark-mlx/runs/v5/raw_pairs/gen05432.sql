WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, HOUR(started_at) AS started_at_hour
    FROM sessions
    WHERE country IN ('JP', 'US', 'UK')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(started_at_hour) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS started_at_hour_count_7d,
        NTILE(4) OVER (PARTITION BY country, customer_id ORDER BY started_at_hour DESC, id) AS started_at_hour_bucket,
        FIRST_VALUE(started_at_hour, TRUE) OVER (PARTITION BY device ORDER BY started_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_started_at_hour,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY browser ORDER BY started_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        AVG(started_at_hour) OVER (PARTITION BY browser ORDER BY started_at_hour, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS started_at_hour_avg_6
    FROM cleaned
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country, customer_id ORDER BY started_at DESC NULLS LAST) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
),
cleaned_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        first_known_started_at_hour - started_at_hour_avg_6 AS first_known_started_at_hour_minus_started_at_hour_avg_6,
        COALESCE(browser, 'NONE') AS browser_filled
    FROM ranked
    WHERE browser IS NOT DISTINCT FROM 'Chrome'
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.web_event_event_time,
        latest.page
    FROM cleaned_sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        device,
        started_at,
        started_at_hour,
        started_at_hour_count_7d,
        started_at_hour_bucket,
        rn,
        web_event_event_time,
        DENSE_RANK() OVER (PARTITION BY browser ORDER BY started_at_hour_count_7d DESC NULLS FIRST) AS started_at_hour_count_7d_dense_rank,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY country ORDER BY started_at_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        SUM(started_at_hour_bucket) OVER (PARTITION BY page ORDER BY started_at_hour_count_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_started_at_hour_bucket
    FROM latest_match
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    DATE_TRUNC('WEEK', started_at) AS week_start,
    CASE WHEN started_at_hour_count_7d >= 12 THEN 'HIGH' ELSE 'LOW' END AS started_at_hour_count_7d_tier,
    MIN(web_event_event_time) AS started_at,
    ROUND(SUM(started_at_hour_count_7d) / NULLIF(COUNT(CASE WHEN device = 'desktop' THEN 1 END), 0), 2) AS started_at_hour_count_7d_per_desktop,
    PERCENTILE_APPROX(cumulative_started_at_hour_bucket, 0.5) AS cumulative_started_at_hour_bucket_p50,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    GROUPING(DATE_TRUNC('QUARTER', started_at)) AS gid
FROM metrics
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', started_at), DATE_TRUNC('WEEK', started_at), CASE WHEN started_at_hour_count_7d >= 12 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY cumulative_started_at_hour_bucket_p50 DESC NULLS LAST
