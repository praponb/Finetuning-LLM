SELECT o.id, o.status, o.amount, o.channel FROM orders o WHERE o.amount = (SELECT MIN(o2.amount) FROM orders o2 WHERE o2.status = o.status) ORDER BY o.channel, o.status
