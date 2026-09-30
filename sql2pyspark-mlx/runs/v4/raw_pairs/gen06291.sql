SELECT
    id,
    status,
    customer_id,
    balance,
    INITCAP(TRIM(status)) AS status_title,
    CONCAT(status, ' - ', account_type) AS status_account_type
FROM accounts
WHERE balance > 56000 AND status IN ('CLOSED', 'FROZEN', 'OPEN')
ORDER BY id DESC
