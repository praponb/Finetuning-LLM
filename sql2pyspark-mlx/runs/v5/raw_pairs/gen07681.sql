SELECT
    id,
    customer_id,
    balance,
    status,
    ADD_MONTHS(opened_date, 6) opened_date_plus_months
FROM accounts
WHERE account_type IN ('CREDIT', 'LOAN')
ORDER BY status DESC;
