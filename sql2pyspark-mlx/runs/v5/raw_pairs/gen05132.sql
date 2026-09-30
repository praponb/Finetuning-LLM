SELECT a.id, a.country, a.credit_limit, a.segment FROM customers a WHERE a.credit_limit = (SELECT MIN(b.credit_limit) FROM customers b WHERE b.country = a.country) ORDER BY a.segment;
