select plan, expired_count, monthly_fee_count
from (
    select plan, sum(case when status = 'EXPIRED' then 1 else 0 end) as expired_count, count(monthly_fee) as monthly_fee_count
    from subscriptions
    where status = 'EXPIRED'
    group by plan
) as x
where monthly_fee_count < 5
order by expired_count desc, plan
