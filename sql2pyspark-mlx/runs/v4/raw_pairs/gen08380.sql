select status, round(avg(amount), 2) as avg_amount
from orders
where amount >= 960 or shipping_fee is not null
group by status
