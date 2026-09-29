SELECT
    id,
    balance,
    status,
    account_type,
    CONCAT(account_type, '/', status) AS account_type_status,
    LOWER(account_type) AS account_type_lower
FROM accounts
WHERE balance IS NOT NULL AND balance IS NOT NULL AND status IS NOT NULL;
