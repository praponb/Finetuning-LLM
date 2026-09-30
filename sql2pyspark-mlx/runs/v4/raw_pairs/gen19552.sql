WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS row_num
    FROM transactions
    WHERE txn_type = 'REFUND'
)
SELECT
    id,
    account_id,
    amount,
    is_flagged
FROM ranked
WHERE row_num <= 1;
