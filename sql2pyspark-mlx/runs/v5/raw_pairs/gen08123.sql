SELECT device, country FROM sessions WHERE browser NOT IN ('Safari', 'Firefox', 'Edge') ORDER BY country DESC;
