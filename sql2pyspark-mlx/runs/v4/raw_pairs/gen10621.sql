select customer_id, country, upper(device) as device_upper from sessions where country = 'UK';
