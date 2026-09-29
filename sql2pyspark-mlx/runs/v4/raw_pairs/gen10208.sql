WITH kept_keys AS (
    -- Set difference on keys
    SELECT status, account_type
    FROM accounts
    EXCEPT
    SELECT status, account_type
    FROM accounts
    WHERE status IN ('CLOSED', 'FROZEN', 'OPEN')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM accounts s
    INNER JOIN kept_keys k ON s.status = k.status AND s.account_type = k.account_type
)
SELECT
    id,
    customer_id,
    account_type,
    opened_date
FROM verified
ORDER BY account_type DESC
