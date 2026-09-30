SELECT *, age + credit_limit AS age_plus_credit_limit FROM customers WHERE city IN ('Tokyo', 'Berlin') ORDER BY id;
