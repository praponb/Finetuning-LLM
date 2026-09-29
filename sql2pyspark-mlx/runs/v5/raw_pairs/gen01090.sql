WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY started_at NULLS FIRST, id) AS dedup_rank
    FROM sessions
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    started_at,
    country
FROM ranked
ORDER BY started_at, country;
