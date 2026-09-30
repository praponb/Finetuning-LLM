SELECT dim.city, s.status, COUNT(*) AS cnt, MIN(s.balance) AS min_balance FROM accounts s INNER JOIN customers dim ON s.customer_id = dim.id GROUP BY dim.city, s.status ORDER BY city;
