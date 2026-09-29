WITH kept_keys AS (
    SELECT id, country
    FROM sessions
    EXCEPT
    SELECT id, country
    FROM sessions
    WHERE device = 'tablet'
),
verified AS (
    SELECT s.*
    FROM sessions s
    INNER JOIN kept_keys k ON s.id = k.id AND s.country = k.country
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at
FROM verified
ORDER BY id DESC NULLS LAST;
