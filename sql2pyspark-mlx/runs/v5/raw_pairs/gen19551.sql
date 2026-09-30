select
    customer_id,
    case when monthly_fee >= 184 then 'high' when monthly_fee >= 117 then 'mid' else 'low' end as monthly_fee_bucket,
    round((100 * sum(case when plan = 'premium' then 1 else 0 end)) / count(*), 1) as pct_premium,
    min(start_date) as first_start_date,
    sum(case when status = 'EXPIRED' then 1 else 0 end) as expired_count
from subscriptions
group by customer_id, case when monthly_fee >= 184 then 'high' when monthly_fee >= 117 then 'mid' else 'low' end
