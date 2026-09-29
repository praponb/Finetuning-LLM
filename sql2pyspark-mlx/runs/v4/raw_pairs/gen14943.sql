SELECT a.id, a.status, a.discount, a.amount FROM orders a WHERE a.discount = (SELECT MIN(b.discount) FROM orders b WHERE b.status = a.status);
