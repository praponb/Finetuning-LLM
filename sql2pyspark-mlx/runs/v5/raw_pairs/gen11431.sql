SELECT id, balance FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance DESC, id) AS row_num FROM accounts) ranked WHERE row_num = 1
