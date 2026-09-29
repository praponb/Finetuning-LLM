WITH numbered AS (
    SELECT
        id,
        txn_type,
        amount,
        account_id,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) AS pos
    FROM transactions
    WHERE merchant IN ('7-Eleven', 'Grab', 'Starbucks')
)
SELECT
    id,
    txn_type,
    amount,
    account_id
FROM numbered
WHERE pos <= 2
ORDER BY amount ASC
