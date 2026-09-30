WITH src AS (
    SELECT *
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 12 AND 17 AND (txn_type IN ('CREDIT', 'REFUND') OR txn_type = 'CREDIT')
),
Summary AS (
    SELECT
        account_id,
        is_flagged,
        SUM(CASE WHEN merchant = 'Grab' THEN amount ELSE 0 END) AS grab_amount,
        COUNT(*) AS row_count
    FROM src
    GROUP BY account_id, is_flagged
)
SELECT
    account_id,
    is_flagged,
    grab_amount,
    row_count
FROM Summary
WHERE row_count < 6
ORDER BY row_count DESC;
