SELECT id, balance, NTILE(10) OVER (ORDER BY balance, id) quartile FROM accounts
