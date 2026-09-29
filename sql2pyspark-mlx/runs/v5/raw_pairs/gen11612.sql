WITH TopWebEvents AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC) AS rnk
    FROM web_events
)
SELECT id, page, duration_sec
FROM TopWebEvents
WHERE rnk <= 1
ORDER BY id DESC, duration_sec
