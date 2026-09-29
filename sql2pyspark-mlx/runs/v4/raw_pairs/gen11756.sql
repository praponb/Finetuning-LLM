WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM customers
    INTERSECT
    SELECT id
    FROM customers
    WHERE is_active
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    segment,
    signup_date,
    is_active,
    credit_limit,
    age
FROM kept_rows
ORDER BY signup_date NULLS FIRST;
