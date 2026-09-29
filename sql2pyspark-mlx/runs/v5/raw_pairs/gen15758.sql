SELECT launch_date event_date FROM products INTERSECT SELECT opened_date event_date FROM accounts ORDER BY event_date;
