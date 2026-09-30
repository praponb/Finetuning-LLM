select
    date_format(start_date, 'yyyy-MM') as start_month,
    status,
    case when monthly_fee >= 206 then 'high' when monthly_fee >= 104 then 'mid' else 'low' end as monthly_fee_bucket,
    concat_ws(', ', sort_array(collect_set(status))) as status_values,
    sum(case when plan = 'standard' then monthly_fee else 0 end) as standard_monthly_fee
from subscriptions
group by 1, 2, 3
