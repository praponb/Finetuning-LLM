WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS rank_in_group
        FROM sessions
    ) ranked
    WHERE rank_in_group = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device,
        country,
        rank_in_group,
        LENGTH(country) AS country_length,
        COALESCE(country, 'DIRECT') AS country_clean
    FROM eligible
    WHERE country IS NOT NULL
      AND started_at >= '2024-08-01'
)
SELECT
    id,
    device,
    rank_in_group,
    country_length,
    country_clean
FROM valid
ORDER BY id DESC NULLS FIRST;
