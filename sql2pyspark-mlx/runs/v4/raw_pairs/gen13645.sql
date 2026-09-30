SELECT c.id, c.age, c.city, c.signup_date FROM customers c WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.customer_id = c.id AND s.country IN ('TH', 'UK', 'US'));
