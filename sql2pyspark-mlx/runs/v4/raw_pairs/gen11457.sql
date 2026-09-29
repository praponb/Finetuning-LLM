WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CAST(duration_sec AS STRING) duration_sec_num,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') event_time_label,
        COALESCE(page, 'NONE') resolved_page
    FROM web_events
    WHERE event_time >= DATE '2023-10-17'
      AND page IS NOT NULL
      AND session_id IN (6, 7, 30)
)
-- QUALIFY-style window filter
SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY duration_sec DESC NULLS FIRST, id) AS row_num
FROM base
QUALIFY row_num = 1;
