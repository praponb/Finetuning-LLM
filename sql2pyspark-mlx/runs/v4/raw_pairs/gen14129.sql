select c.id, c.credit_limit from customers c where exists (select 1 from subscriptions s where s.customer_id = c.id);
