SELECT is_active, signup_date, CASE WHEN segment IN ('Consumer', 'Corporate') THEN 'Priority' ELSE 'Other' END AS segment_group FROM customers ORDER BY segment_group ASC;
