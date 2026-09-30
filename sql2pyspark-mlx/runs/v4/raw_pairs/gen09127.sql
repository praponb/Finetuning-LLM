WITH valid_transactions AS (
    SELECT *
    FROM transactions
    WHERE merchant IN ('Shell', 'Amazon', 'Starbucks') AND merchant IN ('Lazada', 'Grab', 'Shell') AND txn_time < '2023-08-02'
),
agg AS (
    SELECT txn_type, MAX(txn_time) AS latest_txn_time
    FROM valid_transactions
    GROUP BY txn_type
)
SELECT *
FROM agg
ORDER BY txn_type
