SELECT country, COALESCE(browser, 'unassigned') AS browser_filled
FROM sessions
WHERE browser = 'Firefox' AND (started_at >= '2025-09-01' OR device = 'mobile')
