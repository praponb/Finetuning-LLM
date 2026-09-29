select customer_id, round((100 * sum(case when plan = 'enterprise' then 1 else 0 end)) / count(*), 1) as pct_enterprise, sort_array(collect_set(status)) as status_list
from subscriptions
group by customer_id
order by customer_id desc
