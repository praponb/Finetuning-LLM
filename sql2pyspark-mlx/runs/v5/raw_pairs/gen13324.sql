SELECT
    id,
    account_type,
    customer_id,
    opened_date,
    ROW_NUMBER() OVER (PARTITION BY account_type, customer_id ORDER BY opened_date DESC, id) rn
FROM accounts
