WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY started_at DESC) AS row_num
    FROM sessions
)
SELECT *
FROM numbered
WHERE row_num <= 2;
