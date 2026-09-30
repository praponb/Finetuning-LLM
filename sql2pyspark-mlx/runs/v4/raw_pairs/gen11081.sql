SELECT country FROM customers WHERE credit_limit >= 9800 EXCEPT SELECT country FROM sessions;
