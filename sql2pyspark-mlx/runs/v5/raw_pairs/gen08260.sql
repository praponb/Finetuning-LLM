select o.id, o.amount from orders o where o.customer_id in (select id from customers where name like 'Oli%');
