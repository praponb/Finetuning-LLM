SELECT subscriptions.id, subscriptions.status FROM subscriptions WHERE subscriptions.customer_id IN (SELECT id FROM customers WHERE LENGTH(name) >= 10);
