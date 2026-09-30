SELECT
    id,
    account_type,
    opened_date,
    customer_id,
    CONCAT_WS('/', account_type, status) AS account_type_status_key
FROM accounts;
