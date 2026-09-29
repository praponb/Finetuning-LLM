select id, name, segment, credit_limit from customers where signup_date > '2025-09-01' and (is_active = true or age >= 57);
