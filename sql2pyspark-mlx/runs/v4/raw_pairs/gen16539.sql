SELECT country, is_active, LAST_DAY(signup_date) AS signup_date_month_end, UPPER(name) AS name_upper FROM customers WHERE email LIKE '%@company.co'
