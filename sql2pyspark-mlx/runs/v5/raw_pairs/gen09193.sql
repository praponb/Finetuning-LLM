select method, min(amount) min_amount
from payments
where order_id in (6, 24, 28)
group by method
having min(amount) < 560
order by 1
limit 20
