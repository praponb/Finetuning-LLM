select amount, 'payment' as source
from payments
union
select price as amount, 'product' as source
from products
where category != 'Electronics';
