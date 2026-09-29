SELECT *
FROM (
    SELECT
        txn_type,
        merchant,
        amount
    FROM transactions
) src
PIVOT (
    MAX(amount) AS amount,
    COUNT(1) AS cnt
    FOR merchant IN ('Lazada' AS lazada, 'Amazon' AS amazon)
)
