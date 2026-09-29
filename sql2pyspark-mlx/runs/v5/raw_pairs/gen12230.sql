SELECT *, CAST(started_at AS DATE) AS started_at_day, CAST(started_at AS DATE) AS started_at_day_2
FROM sessions
WHERE customer_id IS NULL AND customer_id IN (13, 27, 39) AND browser IN ('Edge', 'Chrome')
ORDER BY device;
