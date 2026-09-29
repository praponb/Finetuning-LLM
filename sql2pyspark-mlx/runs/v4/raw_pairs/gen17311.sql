WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        CAST(duration_sec AS DOUBLE) AS duration_sec_num
    FROM web_events
    WHERE page IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id ASC) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
latest_web_events AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id ASC) AS latest_rank
    FROM latest
    QUALIFY latest_rank = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        session_id,
        event_type,
        duration_sec_num
    FROM latest_web_events
) src
PIVOT (
    MAX(duration_sec_num) AS total,
    COUNT(1) AS cnt
    FOR event_type IN ('search', 'click')
);
