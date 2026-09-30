SELECT
    id,
    customer_id,
    started_at,
    browser
FROM sessions
WHERE browser = 'Safari'
