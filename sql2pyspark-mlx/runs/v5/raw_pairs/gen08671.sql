select s.id, s.status, s.end_date from subscriptions s where s.customer_id not in (select id from customers where name like '%Carol%');
