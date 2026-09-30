WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END AS is_new_york,
        CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END AS is_bangkok,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM customers
    WHERE signup_date > DATE '2023-11-06'
)
SELECT country, id
FROM filtered
INTERSECT
SELECT country, id
FROM customers
WHERE LENGTH(name) > 9;
