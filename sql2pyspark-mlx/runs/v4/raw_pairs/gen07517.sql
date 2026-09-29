SELECT
    id,
    device,
    started_at,
    LENGTH(device) AS device_length
FROM sessions
WHERE started_at >= '2023-05-01' OR country IS NOT NULL;
