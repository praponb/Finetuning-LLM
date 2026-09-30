SELECT *
FROM (
    SELECT txn_type, COUNT(amount) AS amount_count
    FROM transactions
    WHERE txn_type = 'CREDIT'
    GROUP BY txn_type
) AS t
WHERE amount_count >= 4
