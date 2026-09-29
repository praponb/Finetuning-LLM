SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY account_type ORDER BY opened_date DESC) AS rn
    FROM accounts
) x
WHERE rn = 1;
