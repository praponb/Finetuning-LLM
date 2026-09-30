SELECT monthly_fee FROM subscriptions WHERE status = 'ACTIVE' INTERSECT SELECT cost FROM shipments;
