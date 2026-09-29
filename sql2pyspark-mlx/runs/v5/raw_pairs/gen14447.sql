WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) rn
    FROM sessions
)
SELECT *
FROM ranked
WHERE rn = 1;
