WITH kept_keys AS (
    SELECT txn_type, merchant
    FROM transactions
    INTERSECT ALL
    SELECT txn_type, merchant
    FROM transactions
    WHERE txn_time >= '2025-02-01'
),
kept_rows AS (
    SELECT s.*
    FROM transactions s
    INNER JOIN kept_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
)
SELECT
    id,
    amount,
    txn_time,
    merchant,
    CASE WHEN merchant RLIKE '^(Lazada|Grab)$' THEN 'CORE' WHEN merchant RLIKE '^(7-Eleven|Amazon)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS merchant_zone
FROM kept_rows
ORDER BY amount;
