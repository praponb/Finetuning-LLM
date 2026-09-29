SELECT id, opened_date, INITCAP(TRIM(account_type)) AS account_type_title, CONCAT_WS('/', status, account_type) AS status_account_type_key FROM accounts
