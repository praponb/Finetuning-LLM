SELECT
    status,
    account_type,
    customer_id,
    TRIM(account_type) clean_account_type
FROM accounts
WHERE balance < 69000
ORDER BY account_type, clean_account_type DESC
