SELECT
    id,
    account_type,
    balance,
    opened_date
FROM (
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) rn
    FROM accounts
    WHERE opened_date < '2025-04-01'
) r
WHERE rn = 1
