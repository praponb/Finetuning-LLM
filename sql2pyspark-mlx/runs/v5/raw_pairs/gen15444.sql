SELECT
    id,
    account_type,
    balance,
    opened_date,
    AVG(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS balance_avg_5,
    SUM(balance) OVER (PARTITION BY account_type) AS account_type_sum_balance
FROM accounts
WHERE account_type NOT IN ('CREDIT', 'LOAN', 'CHECKING');
