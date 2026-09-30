SELECT id, customer_id, country, LENGTH(device) AS device_length, CASE WHEN browser IN ('Safari', 'Edge') THEN 'Tier 1' ELSE 'Other' END AS browser_group FROM sessions;
