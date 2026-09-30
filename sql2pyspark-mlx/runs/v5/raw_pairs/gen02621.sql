SELECT id, started_at
FROM sessions
WHERE device IN ('mobile', 'desktop', 'tablet') AND started_at < '2024-01-01' AND customer_id IS NULL;
