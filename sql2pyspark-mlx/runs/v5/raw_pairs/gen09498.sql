SELECT *
FROM (
    SELECT
        account_id,
        txn_type,
        merchant,
        amount
    FROM transactions
) src
PIVOT (
    AVG(amount)
    FOR merchant IN ('Grab' AS grab, 'Shell' AS shell)
);
