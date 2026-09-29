select shipping_fee
from orders
where status = 'COMPLETED'
union
select cost
from products;
