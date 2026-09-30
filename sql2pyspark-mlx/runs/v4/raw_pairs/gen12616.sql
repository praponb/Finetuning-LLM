WITH filtered_transactions AS (
    SELECT *, LOWER(txn_type) AS txn_type_lower
    FROM transactions
    WHERE txn_type != 'FEE'
),
transactions_stats AS (
    SELECT merchant, SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END) AS lazada_count, SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount
    FROM filtered_transactions
    GROUP BY merchant
)
SELECT *
FROM transactions_stats;
