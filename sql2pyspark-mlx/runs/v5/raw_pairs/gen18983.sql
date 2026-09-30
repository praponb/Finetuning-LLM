select
    status,
    case when plan in ('premium', 'enterprise') then 'priority' else 'other' end as plan_group,
    sum(monthly_fee) as total_monthly_fee,
    count(*) as num_subscriptions,
    sort_array(collect_set(plan)) as plan_list
from subscriptions
where status in ('CANCELLED', 'ACTIVE', 'PAUSED') and plan not in ('premium', 'basic', 'standard')
group by status, case when plan in ('premium', 'enterprise') then 'priority' else 'other' end
