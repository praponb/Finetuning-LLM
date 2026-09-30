select s.customer_id, s.browser, s.device, c.city, c.segment from sessions s full outer join customers c on s.customer_id = c.id where s.country = 'SG' and c.is_active = true;
