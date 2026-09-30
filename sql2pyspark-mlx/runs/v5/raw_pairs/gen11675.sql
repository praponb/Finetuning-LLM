SELECT id, signup_date, name, credit_limit, INITCAP(TRIM(country)) AS country_title, DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label FROM customers
