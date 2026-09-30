SELECT *, CASE WHEN status IN ('EXPIRED', 'PAUSED') THEN 'Group A' ELSE 'Other' END AS status_group FROM subscriptions WHERE start_date < '2023-07-01'
