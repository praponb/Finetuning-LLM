SELECT *
FROM (
    SELECT
        account_id,
        merchant,
        amount
    FROM transactions
) src
PIVOT (
    AVG(amount) AS value_sum,
    COUNT(1) AS n
    FOR merchant IN ('7-Eleven' AS v_7_eleven, 'Amazon' AS amazon)
)
