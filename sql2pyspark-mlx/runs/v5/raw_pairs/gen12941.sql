SELECT s.id, s.customer_id, s.monthly_fee FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE country != 'TH')
