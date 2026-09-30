SELECT a.id, a.status, a.discount, a.channel FROM orders a WHERE a.discount > (SELECT AVG(b.discount) FROM orders b WHERE b.status = a.status)
