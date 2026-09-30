SELECT a.id, a.plan, a.monthly_fee FROM subscriptions a WHERE a.monthly_fee < (SELECT AVG(b.monthly_fee) FROM subscriptions b WHERE b.plan = a.plan) ORDER BY a.monthly_fee DESC;
