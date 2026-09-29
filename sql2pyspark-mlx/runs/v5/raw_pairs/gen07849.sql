SELECT
    id,
    opened_date,
    customer_id,
    status
FROM accounts
WHERE balance <= 71000
ORDER BY opened_date;
