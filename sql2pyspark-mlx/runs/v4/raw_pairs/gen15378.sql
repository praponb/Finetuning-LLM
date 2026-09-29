SELECT id, country, credit_limit, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS credit_limit_row FROM customers WHERE is_active ORDER BY id;
