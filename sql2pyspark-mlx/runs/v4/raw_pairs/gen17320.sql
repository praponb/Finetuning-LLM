SELECT
    id,
    device,
    customer_id,
    browser
FROM sessions
WHERE device IN ('tablet', 'mobile');
