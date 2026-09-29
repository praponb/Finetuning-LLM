SELECT id, credit_limit, ROW_NUMBER() OVER (ORDER BY credit_limit, id) AS credit_limit_row FROM customers
