SELECT a.*, d.signup_date, d.segment, d.name FROM sessions a LEFT JOIN customers d ON a.customer_id = d.id
