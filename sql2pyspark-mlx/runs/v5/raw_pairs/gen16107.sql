SELECT *
FROM (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS rnk
    FROM transactions
    WHERE account_id IN (1, 29)
) AS t
WHERE rnk = 1;
