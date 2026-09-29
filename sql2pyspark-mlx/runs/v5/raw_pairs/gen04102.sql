WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount NULLS FIRST, id) AS row_num
    FROM transactions
    QUALIFY row_num = 1
)
SELECT
    id,
    txn_time,
    merchant,
    is_flagged,
    row_num,
    CASE WHEN merchant RLIKE '^(Grab|Amazon)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS merchant_zone
FROM latest;
