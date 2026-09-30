SELECT started_at, browser
FROM sessions
WHERE NOT (browser = 'Edge' OR customer_id = 34)
ORDER BY browser DESC
