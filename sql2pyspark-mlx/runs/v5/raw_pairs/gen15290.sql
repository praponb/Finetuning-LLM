SELECT s.id, s.device, c.is_active, c.age, c.email FROM sessions s JOIN customers c ON s.customer_id = c.id;
