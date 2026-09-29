SELECT
    id,
    status,
    balance,
    NTILE(2) OVER (PARTITION BY status ORDER BY balance, id) AS balance_ntile
FROM accounts;
