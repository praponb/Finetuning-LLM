SELECT a.id, a.status, a.monthly_fee, a.customer_id FROM subscriptions a WHERE a.monthly_fee < (SELECT AVG(b.monthly_fee) FROM subscriptions b WHERE b.status = a.status) ORDER BY a.id DESC;
