SELECT *
FROM (
    SELECT
        account_id,
        merchant,
        amount
    FROM transactions
) src
PIVOT (
    SUM(amount) AS amount
    FOR merchant IN ('7-Eleven' AS v_7_eleven, 'Starbucks' AS starbucks, 'Lazada' AS lazada)
);
