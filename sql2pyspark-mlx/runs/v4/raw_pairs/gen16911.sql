SELECT
    id,
    device,
    country,
    CAST(started_at AS DATE) AS started_at_day,
    LOWER(device) AS device_lower
FROM sessions
WHERE device IN ('desktop', 'tablet') AND customer_id IN (18, 20, 29, 32) AND country IN ('TH', 'DE');
