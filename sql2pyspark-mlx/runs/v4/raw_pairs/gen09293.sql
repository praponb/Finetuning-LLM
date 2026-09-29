SELECT *, TRIM(account_type) clean_account_type, CASE WHEN status IN ('FROZEN', 'OPEN') THEN 'Priority' ELSE 'Other' END status_group FROM accounts WHERE balance > 26000
