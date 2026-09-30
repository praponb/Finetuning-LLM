WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        started_at,
        country,
        DATE_TRUNC('month', started_at) AS started_at_period
    FROM sessions
    WHERE customer_id IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
    ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY started_at DESC, id ASC) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY device, country ORDER BY started_at_period ASC, id ASC) AS event_seq_2
FROM valid
