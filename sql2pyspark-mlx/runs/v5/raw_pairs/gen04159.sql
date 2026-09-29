select method, order_id, count(distinct amount) as unique_methods
from payments
group by method, order_id
having count(amount) >= 3;
