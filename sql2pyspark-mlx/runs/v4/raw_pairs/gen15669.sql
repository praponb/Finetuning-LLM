SELECT c.id, c.age FROM customers c WHERE c.id IN (SELECT customer_id FROM subscriptions WHERE monthly_fee > 230) ORDER BY id ASC, age DESC
