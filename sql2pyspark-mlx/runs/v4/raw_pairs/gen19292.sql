SELECT id, merchant, amount
FROM (
    SELECT
        id,
        merchant,
        amount,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS pos
    FROM transactions
) AS r
WHERE pos <= 3;
