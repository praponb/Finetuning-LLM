SELECT
    id,
    page,
    duration_sec,
    DENSE_RANK() OVER (PARTITION BY page ORDER BY duration_sec) AS duration_sec_dense_rank
FROM web_events
WHERE session_id IN (13, 16)
