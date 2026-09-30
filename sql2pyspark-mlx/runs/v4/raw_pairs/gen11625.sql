SELECT *, SUBSTRING(plan, 1, 2) AS plan_prefix, DATE_SUB(end_date, 81) AS end_date_minus_81d
FROM subscriptions
WHERE status = 'EXPIRED';
