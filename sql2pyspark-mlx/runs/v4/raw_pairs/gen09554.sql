select status, min(amount) as min_amount
from payments
where order_id = 9
group by status
having avg(amount) >= 490
order by status desc, min_amount desc;
