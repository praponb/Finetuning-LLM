SELECT status FROM shipments INTERSECT SELECT status FROM subscriptions WHERE monthly_fee > 120;
