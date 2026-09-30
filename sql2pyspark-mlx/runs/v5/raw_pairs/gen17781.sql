WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DAYOFWEEK(event_time) AS event_time_dayofweek,
        UPPER(event_type) AS event_type_upper
    FROM web_events
    WHERE session_id = 27
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(duration_sec) OVER (PARTITION BY page ORDER BY event_time_dayofweek, id) AS next_duration_sec,
        LAG(duration_sec) OVER (PARTITION BY page ORDER BY event_time_dayofweek, id) AS prev_duration_sec,
        LAG(event_time_dayofweek) OVER (PARTITION BY page ORDER BY event_time_dayofweek, id) AS prev_event_time_dayofweek,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time_dayofweek, id) AS row_seq
    FROM cleaned
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.event_time_dayofweek,
        s.event_type_upper,
        s.prev_duration_sec,
        p2.browser,
        p2.country,
        p2.device
    FROM enriched s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time_dayofweek,
        event_type_upper,
        prev_duration_sec,
        browser,
        country,
        SUBSTRING(country, 1, 3) AS country_prefix,
        COALESCE(country, 'NONE') AS resolved_country
    FROM with_dims
    WHERE country IS NOT NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, session_id
    FROM valid
    INTERSECT
    SELECT id, session_id
    FROM web_events
    WHERE session_id IN (3, 32, 35, 39)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id AND s.session_id = k.session_id
)
SELECT
    s.session_id,
    s.event_time_dayofweek,
    s.event_type_upper,
    s.browser,
    s.country,
    s.resolved_country,
    (SELECT MIN(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS min_web_event_duration_sec
FROM verified s
ORDER BY browser DESC
