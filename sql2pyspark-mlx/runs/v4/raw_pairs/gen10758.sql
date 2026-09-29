select
    status,
    plan,
    round((100 * sum(case when plan = 'standard' then 1 else 0 end)) / count(*), 1) pct_standard,
    concat_ws(', ', sort_array(collect_set(status))) status_values
from subscriptions
where end_date < '2024-11-01' or customer_id = 19
group by status, plan
