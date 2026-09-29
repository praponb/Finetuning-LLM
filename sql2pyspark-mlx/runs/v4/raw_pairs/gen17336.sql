SELECT end_date FROM subscriptions WHERE status IN ('ACTIVE', 'EXPIRED') EXCEPT SELECT end_date FROM subscriptions WHERE monthly_fee < 180
