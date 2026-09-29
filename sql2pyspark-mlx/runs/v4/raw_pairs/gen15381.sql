SELECT signup_date FROM customers WHERE city NOT IN ('London', 'Bangkok', 'Singapore') AND email LIKE '%@yahoo.com' ORDER BY signup_date;
