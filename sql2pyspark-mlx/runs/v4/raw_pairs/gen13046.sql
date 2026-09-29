select id, discount
from orders
where discount = (select min(discount) from orders where amount > 1470)
