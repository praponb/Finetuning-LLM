WITH eligible AS (
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.status IN ('SHIPPED', 'CANCELLED', 'PENDING')
        )
),
ranked AS (
    SELECT *
    FROM eligible
    QUALIFY RANK() OVER (PARTITION BY segment, country ORDER BY age DESC) = 1
),
remaining_keys AS (
    SELECT segment
    FROM ranked
    EXCEPT
    SELECT segment
    FROM customers
    WHERE city = 'Tokyo'
),
verified AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.segment = k.segment
),
not_flagged AS (
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.store_id IN (20, 33)
        )
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    is_active,
    credit_limit,
    age
FROM not_flagged
ORDER BY age DESC
