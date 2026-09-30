SELECT a.id, a.customer_id, a.opened_date, c.country, c.age, UPPER(a.account_type) AS account_type_upper FROM accounts a JOIN customers c ON c.id = a.customer_id
