SELECT segment FROM customers WHERE credit_limit >= 7900 UNION SELECT segment FROM customers WHERE UPPER(email) LIKE '%LIC%'
