SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS row_num FROM accounts) AS t WHERE row_num <= 2
