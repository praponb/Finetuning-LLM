select
    status,
    customer_id,
    case when monthly_fee >= 215 then 'high' when monthly_fee >= 70 then 'mid' else 'low' end as monthly_fee_bucket,
    sort_array(collect_set(status)) as status_list,
    sum(monthly_fee) as total_monthly_fee
from subscriptions
where plan = 'enterprise' or monthly_fee >= 70
group by status, customer_id, case when monthly_fee >= 215 then 'high' when monthly_fee >= 70 then 'mid' else 'low' end
order by monthly_fee_bucket
