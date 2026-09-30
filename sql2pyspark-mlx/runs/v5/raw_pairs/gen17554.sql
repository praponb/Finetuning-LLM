SELECT browser, TO_DATE(started_at) AS started_at_date, CASE WHEN device IN ('mobile', 'tablet') THEN 'Group A' ELSE 'Other' END AS device_group
FROM sessions;
