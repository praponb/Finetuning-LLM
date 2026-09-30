select status, max(amount) as max_amount
from orders
where discount is not null
group by status
