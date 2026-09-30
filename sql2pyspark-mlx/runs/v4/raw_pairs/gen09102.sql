SELECT
    id,
    customer_id,
    opened_date,
    SUBSTRING(status, 1, 2) AS status_prefix
FROM accounts
