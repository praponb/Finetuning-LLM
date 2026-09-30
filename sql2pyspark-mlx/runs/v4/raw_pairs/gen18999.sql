select order_id, sum(case when status = 'SUCCESS' then 1 else 0 end) as success_count
from payments
where hour(paid_at) between 6 and 10
group by order_id
order by order_id
