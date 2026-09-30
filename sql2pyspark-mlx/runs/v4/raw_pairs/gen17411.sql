SELECT *
FROM (
    SELECT
        txn_type,
        account_id,
        merchant,
        amount
    FROM transactions
) src
PIVOT (
    AVG(amount)
    FOR merchant IN ('Shell' AS shell, '7-Eleven' AS v_7_eleven)
);
