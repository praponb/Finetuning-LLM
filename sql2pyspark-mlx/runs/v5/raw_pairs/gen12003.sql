select *
from (
    select plan, status, max(monthly_fee) - min(monthly_fee) as monthly_fee_range
    from subscriptions
    group by plan, status
) as sub
where monthly_fee_range >= 60;
