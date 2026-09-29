SELECT
    status,
    account_type,
    opened_date,
    TRIM(account_type) clean_account_type
FROM accounts;
