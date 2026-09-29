WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, session_id
    FROM web_events
    EXCEPT
    SELECT id, session_id
    FROM web_events
    WHERE event_type = 'purchase'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.session_id = k.session_id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(event_type) AS event_type_length,
        CONCAT(event_type, ' - ', page) AS event_type_page
    FROM verified
    WHERE page IS NOT NULL
      AND page IN ('/product', '/home', '/search')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM cleaned
    INTERSECT ALL
    SELECT id
    FROM web_events
    WHERE event_type IN ('click', 'page_view')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.event_time,
        a.event_type_page,
        p.browser
    FROM kept_rows a
    INNER JOIN sessions p
        ON a.session_id = p.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, WEEKOFYEAR(event_time) AS event_time_weekofyear
    FROM joined
    WHERE event_time < '2023-04-01'
)
SELECT
    s.id,
    s.session_id,
    s.event_time,
    (SELECT SUM(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS sum_web_event_duration_sec
FROM valid s
ORDER BY id DESC NULLS LAST;
