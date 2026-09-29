WITH filtered AS (
    SELECT
        id,
        event_type,
        duration_sec,
        ROUND(duration_sec, 1) duration_sec_rounded
    FROM web_events
    WHERE page IS NOT NULL
      AND session_id IN (19, 38)
)
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY duration_sec DESC NULLS FIRST, id) = 1;
