SELECT *
FROM (
    SELECT
        status,
        account_type,
        balance
    FROM accounts
) src
PIVOT (
    SUM(balance) AS value_sum,
    COUNT(1) AS cnt
    FOR account_type IN ('CREDIT' AS credit, 'LOAN' AS loan, 'CHECKING' AS checking)
);
