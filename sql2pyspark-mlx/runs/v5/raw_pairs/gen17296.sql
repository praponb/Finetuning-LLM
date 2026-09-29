SELECT id, started_at, device, DATE_TRUNC('YEAR', started_at) AS started_at_period FROM sessions ORDER BY started_at DESC;
