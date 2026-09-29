select id, amount, customer_id, status from orders where amount > (select avg(amount) from orders)
