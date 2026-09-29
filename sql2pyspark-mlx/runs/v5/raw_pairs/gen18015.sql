WITH valid AS (
    SELECT
        id,
        txn_time,
        merchant,
        INITCAP(TRIM(merchant)) AS merchant_title,
        LENGTH(txn_type) AS txn_type_length
    FROM transactions
    WHERE merchant IS NOT DISTINCT FROM 'Shell'
      AND txn_time > TIMESTAMP '2023-07-13 08:00:00'
)
SELECT id, merchant
FROM valid
EXCEPT ALL
SELECT id, merchant
FROM transactions
WHERE is_flagged
