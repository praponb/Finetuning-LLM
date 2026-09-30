SELECT
    id,
    customer_id,
    browser,
    INITCAP(TRIM(device)) AS device_title
FROM sessions
WHERE NOT (customer_id IS NOT NULL OR customer_id = 32)
ORDER BY customer_id, browser
