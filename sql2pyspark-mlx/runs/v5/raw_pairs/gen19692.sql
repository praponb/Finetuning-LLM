SELECT
    id,
    country,
    started_at,
    RANK() OVER (PARTITION BY country ORDER BY started_at) AS rnk,
    RANK() OVER (ORDER BY started_at) AS started_at_rank
FROM sessions
WHERE device = 'tablet';
