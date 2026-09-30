WITH numbered AS (
    SELECT
        id,
        session_id,
        duration_sec,
        RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS rn
    FROM web_events
    WHERE duration_sec IS NULL
)
SELECT id, session_id, duration_sec
FROM numbered
WHERE rn <= 3
ORDER BY id DESC, 3;
