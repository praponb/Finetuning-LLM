SELECT id, country, started_at, CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg FROM sessions WHERE browser IN ('Safari', 'Firefox', 'Chrome');
