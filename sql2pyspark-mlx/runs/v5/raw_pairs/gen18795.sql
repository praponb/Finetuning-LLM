select id, customer_id, started_at, initcap(trim(country)) country_title, trim(device) clean_device from sessions where device = 'tablet' and started_at >= '2024-04-03';
