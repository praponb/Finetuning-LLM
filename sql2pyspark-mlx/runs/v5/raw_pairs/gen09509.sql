WITH matched_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM stores
    EXCEPT
    SELECT city, id
    FROM stores
    WHERE city IN ('Berlin', 'Singapore', 'Tokyo')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM stores s
    INNER JOIN matched_keys k ON s.city = k.city AND s.id = k.id
)
SELECT
    id,
    city,
    opened_date,
    sqft
FROM verified
ORDER BY sqft DESC NULLS FIRST
