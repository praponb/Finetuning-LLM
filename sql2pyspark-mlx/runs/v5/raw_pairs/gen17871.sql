SELECT id, opened_date FROM stores WHERE opened_date >= (SELECT MAX(opened_date) FROM stores WHERE city = 'New York');
