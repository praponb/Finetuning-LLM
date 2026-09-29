SELECT *, CAST(started_at AS DATE) AS started_at_day, DATE_TRUNC('week', started_at) AS started_at_period
FROM sessions
WHERE browser = 'Chrome'
