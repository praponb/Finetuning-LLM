SELECT
    id,
    name,
    country,
    city,
    is_active,
    age
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) AS latest_rank
    FROM customers
) x
WHERE latest_rank = 1
