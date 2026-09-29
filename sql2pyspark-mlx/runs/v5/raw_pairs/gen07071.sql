WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC, id) AS row_num
        FROM transactions
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    txn_type,
    amount,
    is_flagged,
    CASE WHEN txn_type RLIKE '^(REFUND|CREDIT)$' THEN 'GROUP_B' WHEN txn_type RLIKE '^(DEBIT|FEE)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS txn_type_zone
FROM deduped
