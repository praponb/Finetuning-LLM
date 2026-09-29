WITH Numbered AS (
    SELECT
        id,
        merchant,
        txn_time,
        is_flagged,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS pos
    FROM transactions
)
SELECT *
FROM Numbered
WHERE pos = 1
