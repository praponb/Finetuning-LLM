select
    case when budget >= 3210000 then 'high' when budget >= 1971000 then 'mid' else 'low' end as budget_bucket,
    concat_ws(', ', sort_array(collect_set(location))) as location_values,
    count(distinct budget) as unique_dept_names,
    count(budget) as budget_count
from departments
group by 1
having max(budget) < 3076000
