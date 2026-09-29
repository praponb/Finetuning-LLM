SELECT
    status,
    customer_id,
    account_type,
    INITCAP(TRIM(status)) AS status_title
FROM accounts
WHERE status NOT IN ('FROZEN', 'OPEN')
ORDER BY status_title ASC
