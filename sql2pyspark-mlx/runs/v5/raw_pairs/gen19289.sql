SELECT method, paid_at, HOUR(paid_at) AS paid_at_hour FROM payments WHERE (amount BETWEEN 870 AND 910 AND paid_at >= '2023-04-24') OR status IN ('SUCCESS', 'FAILED') ORDER BY method, paid_at_hour
