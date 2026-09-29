SELECT id, browser, customer_id, device, QUARTER(started_at) AS started_at_quarter FROM sessions;
