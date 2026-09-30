SELECT signup_date, segment FROM customers WHERE name LIKE 'He%' INTERSECT SELECT signup_date, segment FROM customers WHERE credit_limit BETWEEN 8800 AND 9100 ORDER BY signup_date DESC;
