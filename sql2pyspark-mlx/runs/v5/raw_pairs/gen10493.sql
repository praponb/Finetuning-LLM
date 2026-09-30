WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS row_num
        FROM customers
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    name,
    email,
    signup_date,
    is_active,
    credit_limit,
    age,
    row_num
FROM deduped
ORDER BY id DESC
LIMIT 10
