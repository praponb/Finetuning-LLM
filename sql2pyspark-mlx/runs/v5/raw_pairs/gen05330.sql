SELECT a.id, a.status, a.amount, a.order_date FROM orders a WHERE a.amount = (SELECT MAX(b.amount) FROM orders b WHERE b.status = a.status);
