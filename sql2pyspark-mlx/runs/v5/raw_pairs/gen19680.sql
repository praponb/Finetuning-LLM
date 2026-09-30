SELECT
    id,
    status,
    customer_id,
    account_type,
    CONCAT_WS('/', account_type, status) account_type_status_key,
    DATE_FORMAT(opened_date, 'MMM yyyy') opened_date_label
FROM accounts
WHERE account_type = 'LOAN' AND balance > 75000
