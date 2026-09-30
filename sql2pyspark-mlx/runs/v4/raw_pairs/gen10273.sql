select id, cost
from products
where cost = (select min(cost) from products)
order by id desc
