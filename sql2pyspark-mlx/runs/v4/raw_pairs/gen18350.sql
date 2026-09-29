SELECT opened_date, CONCAT(account_type, '/', status) AS account_type_status, LENGTH(account_type) AS account_type_length FROM accounts;
