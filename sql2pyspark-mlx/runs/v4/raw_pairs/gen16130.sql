SELECT s.id, s.status, s.monthly_fee, s.start_date FROM subscriptions s WHERE s.monthly_fee = (SELECT MIN(s2.monthly_fee) FROM subscriptions s2 WHERE s2.status = s.status) ORDER BY s.monthly_fee DESC
