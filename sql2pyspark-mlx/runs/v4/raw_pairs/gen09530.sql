SELECT
    account_type,
    opened_date,
    TRIM(account_type) clean_account_type,
    CONCAT_WS('-', account_type, status) account_type_status_key
FROM accounts
WHERE account_type IN ('LOAN', 'SAVINGS')
ORDER BY account_type_status_key ASC, 1 ASC
