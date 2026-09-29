SELECT id, device, country, started_at, CASE WHEN browser IN ('Chrome', 'Safari') THEN 'Priority' ELSE 'Other' END AS browser_group FROM sessions WHERE country NOT IN ('SG', 'TH', 'UK')
