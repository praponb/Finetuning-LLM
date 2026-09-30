WITH filtered AS (
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        HOUR(event_time) AS event_time_hour
    FROM web_events
    WHERE event_time >= DATE '2023-04-17'
      AND page IS DISTINCT FROM '/cart'
),
cleaned AS (
    SELECT *, DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label
    FROM filtered
    WHERE event_time >= TIMESTAMP '2023-05-30 08:00:00'
),
ranked AS (
    SELECT
        id,
        event_type,
        event_time
    FROM (
        SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS rn
        FROM cleaned
    ) t
    WHERE rn = 1
),
with_windows AS (
    SELECT
        id,
        event_type,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS event_seq_2,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time DESC, id) AS seq_no_2
    FROM ranked
),
kept_keys AS (
    SELECT event_type, id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT event_type, id
    FROM web_events
    WHERE page IN ('/cart', '/product')
),
kept_rows AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.event_type = k.event_type AND s.id = k.id
)
SELECT
    id,
    event_time,
    seq_no,
    row_seq,
    event_seq,
    event_seq_2
FROM kept_rows
ORDER BY event_time DESC NULLS FIRST
