select status, sum(case when plan = 'standard' then monthly_fee else 0 end) as standard_monthly_fee, sort_array(collect_set(plan)) as plan_list from subscriptions group by status
