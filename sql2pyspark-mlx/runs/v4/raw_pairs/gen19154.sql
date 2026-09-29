WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        CASE WHEN balance >= 86000 THEN 'High' WHEN balance >= 14000 THEN 'Medium' ELSE 'Low' END AS balance_band
    FROM accounts
    WHERE opened_date > DATE '2023-04-08'
      AND customer_id IS NOT NULL
)
-- Latest matching transaction per row (LATERAL ... LIMIT 1)
SELECT s.*, latest.amount
FROM filtered s
JOIN LATERAL (
        SELECT c.amount
        FROM transactions c
        WHERE c.account_id = s.id
        ORDER BY c.amount DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE
