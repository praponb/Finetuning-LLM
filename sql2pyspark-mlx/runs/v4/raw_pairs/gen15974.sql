SELECT id, device, browser, SUBSTRING(device, 1, 2) AS device_prefix FROM sessions WHERE browser IN ('Edge', 'Firefox') AND customer_id IN (2, 24) AND device = 'mobile';
