SELECT
    id,
    device,
    customer_id,
    browser
FROM sessions
WHERE browser NOT IN ('Edge', 'Chrome', 'Firefox')
ORDER BY device ASC
