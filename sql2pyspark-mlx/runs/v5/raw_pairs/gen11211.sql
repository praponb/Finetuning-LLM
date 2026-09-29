SELECT opened_date, 'account' AS origin FROM accounts UNION SELECT signup_date, 'customer' AS origin FROM customers
