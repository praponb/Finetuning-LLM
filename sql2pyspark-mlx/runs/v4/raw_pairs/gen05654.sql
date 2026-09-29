select sum(monthly_fee) total_monthly_fee, concat_ws(', ', sort_array(collect_set(plan))) plan_values, count(distinct monthly_fee) unique_plans
from subscriptions
order by unique_plans desc, total_monthly_fee desc;
