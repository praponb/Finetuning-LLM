WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS rnk
    FROM web_events
)
SELECT *
FROM numbered
WHERE rnk <= 2;
