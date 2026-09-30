WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        HOUR(event_time) AS event_time_hour,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        event_time_hour
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time_hour ASC, id ASC) AS dedup_rank
        FROM cleaned
    ) ranked
    WHERE dedup_rank = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM latest
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.duration_sec,
        d.country,
        d.browser,
        d.device
    FROM sessions_web_events s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        duration_sec,
        country,
        browser,
        device,
        LOWER(device) AS device_lower,
        CASE WHEN country IN ('DE', 'UK') THEN 'Priority' ELSE 'Other' END AS country_group
    FROM enriched
    WHERE duration_sec > 280
)
-- Ranking and gap features
SELECT
    *,
    RANK() OVER w_recent AS duration_sec_rank,
    PERCENT_RANK() OVER (PARTITION BY device ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_percent_rank,
    NTILE(4) OVER (PARTITION BY country ORDER BY duration_sec DESC, id ASC) AS duration_sec_bucket,
    AVG(duration_sec) OVER w_grp AS duration_sec_avg_2_prior
FROM base
WINDOW w_recent AS (ORDER BY duration_sec DESC), w_grp AS (ORDER BY duration_sec ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
