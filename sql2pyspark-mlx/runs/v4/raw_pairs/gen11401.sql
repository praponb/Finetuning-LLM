SELECT *
FROM (
    SELECT
        id,
        account_type,
        opened_date,
        status,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS pos
    FROM accounts
) AS r
WHERE pos <= 3
ORDER BY opened_date DESC, account_type DESC;
