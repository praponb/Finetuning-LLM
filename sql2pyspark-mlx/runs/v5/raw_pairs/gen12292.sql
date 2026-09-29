WITH cleaned AS (
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE txn_time > TO_DATE('2023-11-12')
      AND amount IS NOT NULL
)
SELECT id, merchant
FROM cleaned
EXCEPT ALL
SELECT id, merchant
FROM transactions
WHERE merchant = 'Shell'
