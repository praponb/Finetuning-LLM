WITH latest AS (
    SELECT
        id,
        country,
        city
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id) AS rn
        FROM suppliers
    ) x
    WHERE rn = 1
)
SELECT
    id,
    country,
    city
FROM latest
ORDER BY city DESC NULLS FIRST, id;
