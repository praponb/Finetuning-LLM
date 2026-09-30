WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date
    FROM accounts
    WHERE opened_date > TO_DATE('2023-03-29')
)
-- Latest matching transaction per row (LATERAL ... LIMIT 1)
SELECT
    s.*,
    top1.txn_time,
    top1.merchant
FROM cleaned s
LEFT JOIN LATERAL (
        SELECT c.txn_time, c.merchant
        FROM transactions c
        WHERE c.account_id = s.id
        ORDER BY c.txn_time DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
