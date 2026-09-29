SELECT
    merchant,
    txn_type,
    avg_amount,
    amount_count
FROM (
    SELECT
        merchant,
        txn_type,
        ROUND(AVG(amount), 2) AS avg_amount,
        COUNT(amount) AS amount_count
    FROM transactions
    GROUP BY merchant, txn_type
) AS agg
WHERE amount_count >= 8
ORDER BY avg_amount DESC, amount_count;
