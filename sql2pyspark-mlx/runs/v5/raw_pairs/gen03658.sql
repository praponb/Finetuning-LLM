SELECT device FROM sessions WHERE browser NOT IN ('Safari', 'Chrome', 'Edge') UNION ALL SELECT device FROM sessions WHERE started_at >= '2024-09-01' ORDER BY device
