SELECT device, customer_id, UPPER(device) AS device_upper, MONTH(started_at) AS started_at_month FROM sessions
