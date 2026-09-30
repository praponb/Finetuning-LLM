SELECT a.id, a.status, a.monthly_fee FROM subscriptions a WHERE a.monthly_fee = (SELECT MAX(b.monthly_fee) FROM subscriptions b WHERE b.status = a.status)
