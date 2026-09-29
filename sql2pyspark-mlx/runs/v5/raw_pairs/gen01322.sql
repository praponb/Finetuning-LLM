select customers.id, customers.country from customers where not exists (select 1 from orders where orders.customer_id = customers.id and orders.amount >= 1300)
