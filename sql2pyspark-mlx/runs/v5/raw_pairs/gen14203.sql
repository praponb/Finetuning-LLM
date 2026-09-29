WITH base AS (
    SELECT *, COALESCE(age, 0) AS age_filled
    FROM customers
    WHERE segment IS NOT NULL
      AND credit_limit BETWEEN 12600 AND 15800
)
SELECT id
FROM base
INTERSECT
SELECT id
FROM customers
WHERE name IS NOT NULL
