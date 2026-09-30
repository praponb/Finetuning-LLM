SELECT
    id,
    status,
    opened_date,
    LEAD(opened_date, 2) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_opened_date
FROM accounts
ORDER BY next_opened_date;
