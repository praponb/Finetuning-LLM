SELECT sessions.started_at, sessions.country, sessions.device, customers.name, customers.city FROM sessions LEFT JOIN customers ON sessions.customer_id = customers.id;
