select *
from (
    select dept_name, sum(budget) as total_budget
    from departments
    where dept_name in ('Sales', 'HR', 'Marketing')
    group by dept_name
) as t
where total_budget < 7155000
order by total_budget asc, dept_name asc
limit 3;
