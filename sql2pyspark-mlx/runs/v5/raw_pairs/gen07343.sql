WITH with_dims AS (
    SELECT
        s.id,
        s.session_id,
        s.duration_sec,
        d.country
    FROM web_events s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, session_id ORDER BY duration_sec NULLS FIRST, id) AS rank_in_group
    FROM with_dims
    QUALIFY rank_in_group = 1
),
latest AS (
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY session_id ORDER BY rank_in_group) = 1
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY session_id, country ORDER BY rank_in_group DESC) AS row_num
    FROM latest
    QUALIFY row_num = 1
)
SELECT
    s.id,
    s.session_id,
    s.duration_sec,
    s.rank_in_group,
    s.row_num,
    (SELECT AVG(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM deduped s
ORDER BY rank_in_group DESC NULLS LAST;
