WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time, id) AS row_num
    FROM web_events
)
SELECT *
FROM numbered
WHERE row_num = 1
ORDER BY event_type;
