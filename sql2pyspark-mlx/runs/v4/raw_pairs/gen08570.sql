SELECT *
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY device ORDER BY started_at) AS pos
    FROM sessions
) AS r
WHERE pos <= 3
ORDER BY browser;
