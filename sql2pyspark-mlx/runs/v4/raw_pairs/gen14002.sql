SELECT c.city, s.status, s.end_date, s.monthly_fee FROM customers c LEFT JOIN subscriptions s ON s.customer_id = c.id;
