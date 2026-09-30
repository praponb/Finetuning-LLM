SELECT
    id,
    customer_id,
    device,
    browser,
    TO_DATE(started_at) AS started_at_date
FROM sessions
WHERE browser IN ('Chrome', 'Firefox') AND country NOT IN ('SG', 'US', 'TH');
