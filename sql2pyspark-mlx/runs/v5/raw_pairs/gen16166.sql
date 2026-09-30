with cleaned as (
    select *, trim(dept_name) as clean_dept_name
    from departments
    where dept_name = 'HR'
)
select
    *,
    avg(budget) over w_ordered as running_avg_budget,
    sum(budget) over w_ordered as running_sum_budget,
    max(budget) over w_ordered as running_max_budget,
    min(budget) over w_ordered as running_min_budget
from cleaned
window w_ordered as (partition by location order by budget, id rows between unbounded preceding and current row);
