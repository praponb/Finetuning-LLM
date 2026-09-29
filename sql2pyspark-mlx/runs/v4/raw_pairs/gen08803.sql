SELECT id, opened_date, UPPER(status) AS status_upper, CONCAT(account_type, ' ', status) AS account_type_status FROM accounts WHERE account_type = 'SAVINGS' ORDER BY id DESC;
