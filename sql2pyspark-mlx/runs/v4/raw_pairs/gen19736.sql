select plan, sum(case when status = 'CANCELLED' then monthly_fee else 0 end) as cancelled_monthly_fee
from subscriptions
where plan in ('basic', 'enterprise')
group by plan
