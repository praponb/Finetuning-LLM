select id, amount from orders where amount = (select min(amount) from orders) order by id limit 50
