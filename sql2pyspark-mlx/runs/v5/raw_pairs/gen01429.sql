SELECT signup_date, country FROM customers WHERE segment IN ('Corporate', 'SMB') EXCEPT SELECT signup_date, country FROM customers WHERE LENGTH(name) > 10;
