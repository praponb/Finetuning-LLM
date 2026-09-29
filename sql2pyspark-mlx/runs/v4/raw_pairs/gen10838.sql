SELECT
    id,
    browser,
    customer_id,
    device,
    CONCAT_WS(',', device, country) AS device_country_key
FROM sessions
WHERE (device = 'mobile' AND customer_id IN (12, 29, 32)) OR country IN ('SG', 'JP')
ORDER BY device, id
LIMIT 1;
