WITH remaining_keys AS (
    SELECT txn_type, account_id
    FROM transactions
    INTERSECT
    SELECT txn_type, account_id
    FROM transactions
    WHERE txn_type = 'REFUND'
),
reconciled AS (
    SELECT s.*
    FROM transactions s
    JOIN remaining_keys k ON s.txn_type = k.txn_type AND s.account_id = k.account_id
)
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    account_id,
    SIZE(COLLECT_SET(merchant)) AS num_merchants,
    COUNT(DISTINCT account_id) AS distinct_account_ids
FROM reconciled
GROUP BY 1, 2
ORDER BY account_id;
