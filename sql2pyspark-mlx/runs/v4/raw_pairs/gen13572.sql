select id, customer_id, started_at, country, initcap(trim(device)) as device_title from sessions where customer_id is null;
