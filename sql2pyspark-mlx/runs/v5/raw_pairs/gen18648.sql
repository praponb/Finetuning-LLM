WITH latest AS (
    SELECT
        id,
        name,
        email,
        segment,
        is_active,
        credit_limit
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age NULLS FIRST, id) AS latest_rank
        FROM customers
    ) ranked
    WHERE latest_rank = 1
)
SELECT
    id,
    name,
    email,
    segment,
    credit_limit
FROM latest
