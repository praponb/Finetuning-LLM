SELECT * FROM (SELECT plan, status, monthly_fee FROM subscriptions WHERE status != 'EXPIRED')
PIVOT (COUNT(monthly_fee) FOR status IN ('ACTIVE', 'EXPIRED'));
