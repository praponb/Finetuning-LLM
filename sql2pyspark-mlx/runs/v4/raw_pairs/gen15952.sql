SELECT id, txn_type, amount
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS rn
    FROM transactions
) AS ranked
WHERE rn <= 2
ORDER BY txn_type, amount DESC;
