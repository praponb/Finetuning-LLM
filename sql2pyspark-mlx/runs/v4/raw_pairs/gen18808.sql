SELECT a.id, a.country, a.age, a.credit_limit FROM customers a WHERE a.age = (SELECT MAX(b.age) FROM customers b WHERE b.country = a.country) ORDER BY a.id DESC, a.credit_limit DESC;
