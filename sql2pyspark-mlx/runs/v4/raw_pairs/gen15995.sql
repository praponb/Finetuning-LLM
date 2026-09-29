WITH kept_keys AS (
    SELECT city, id
    FROM customers
    EXCEPT ALL
    SELECT city, id
    FROM customers
    WHERE name LIKE 'bob%'
),
kept_rows AS (
    SELECT s.*
    FROM customers s
    INNER JOIN kept_keys k ON s.city = k.city AND s.id = k.id
)
SELECT
    id,
    name,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    CASE WHEN segment RLIKE '^(Corporate|SMB)$' THEN 'PRIORITY' WHEN segment RLIKE '^(Consumer)$' THEN 'EXTENDED' ELSE 'CORE' END AS segment_zone
FROM kept_rows
