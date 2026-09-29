WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date < s.signup_date
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, segment ORDER BY credit_limit, id) AS latest_rank
    FROM not_flagged
    QUALIFY latest_rank = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at < s.signup_date
        )
)
SELECT
    id,
    name,
    email,
    country,
    city,
    is_active,
    credit_limit,
    age,
    latest_rank
FROM kept
ORDER BY age DESC NULLS LAST;
