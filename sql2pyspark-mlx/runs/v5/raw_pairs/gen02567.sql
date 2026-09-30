select id, amount, discount, customer_id from orders where amount >= (select avg(amount) from orders where customer_id = 35)
