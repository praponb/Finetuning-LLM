WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.account_type = 'LOAN'
        )
)
SELECT
    id,
    name,
    email,
    city,
    signup_date,
    is_active,
    age
FROM kept;
