SELECT id, device, started_at, CASE WHEN browser IN ('Chrome', 'Firefox') THEN 'Core' ELSE 'Other' END AS browser_group FROM sessions;
