SELECT account_id, txn_year, amount_count
FROM (
    SELECT account_id, YEAR(txn_time) AS txn_year, COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY account_id, YEAR(txn_time)
) AS sub
WHERE amount_count < 6;
