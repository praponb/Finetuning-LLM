SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, browser ORDER BY started_at DESC NULLS FIRST, id) AS latest_rank
    FROM sessions
) ranked
WHERE latest_rank = 1;
