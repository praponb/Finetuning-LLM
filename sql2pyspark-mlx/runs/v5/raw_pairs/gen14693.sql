SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) AS row_num
    FROM web_events
) AS t
WHERE row_num <= 2
ORDER BY row_num DESC
