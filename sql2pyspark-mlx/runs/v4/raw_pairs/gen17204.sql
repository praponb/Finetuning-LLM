WITH enriched AS (
    -- Attach sessions attributes
    SELECT a.*, b.country
    FROM web_events a
    LEFT JOIN sessions b
        ON a.session_id = b.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, event_type ORDER BY event_time NULLS LAST, id) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(event_type) AS event_type_upper,
        INITCAP(TRIM(page)) AS page_title
    FROM ranked
    WHERE event_time > DATE '2023-05-24'
      AND country IS NOT NULL
      AND event_type NOT IN ('search', 'purchase')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.country,
    s.dedup_rank,
    s.event_type_upper,
    s.page_title,
    s.burst_id,
    CASE WHEN country RLIKE '^(US|TH)$' THEN 'GROUP_B' ELSE 'CORE' END AS country_zone,
    (SELECT MAX(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS max_web_event_duration_sec
FROM sessionized s
ORDER BY id DESC NULLS LAST;
