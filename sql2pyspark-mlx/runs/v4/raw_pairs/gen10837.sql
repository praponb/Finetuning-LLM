WITH joined AS (
    SELECT a.*, p2.status
    FROM transactions a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    is_flagged,
    CASE WHEN txn_type RLIKE '^(CREDIT|DEBIT)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS txn_type_zone
FROM joined
ORDER BY txn_time NULLS LAST, amount DESC;
