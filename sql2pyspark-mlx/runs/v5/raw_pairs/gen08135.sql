SELECT o.channel, c.signup_date, s.opened_date FROM orders o JOIN customers c ON o.customer_id = c.id JOIN stores s ON s.id = o.store_id WHERE c.name LIKE '%Ford%' ORDER BY o.channel;
