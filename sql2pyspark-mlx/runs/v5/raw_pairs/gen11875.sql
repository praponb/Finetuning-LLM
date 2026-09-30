WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY opened_date, id) AS rn
    FROM accounts
    QUALIFY rn = 1
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    rn
FROM ranked
ORDER BY rn DESC;
