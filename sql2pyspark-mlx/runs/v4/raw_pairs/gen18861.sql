with valid_departments as (
    select *
    from departments
    where dept_name = 'Finance' and dept_name = 'Engineering'
),
summary as (
    select dept_name, case when dept_name in ('Legal', 'Sales') then 'tier_1' else 'other' end as dept_name_group, sum(budget) as total_budget
    from valid_departments
    group by dept_name, case when dept_name in ('Legal', 'Sales') then 'tier_1' else 'other' end
)
select dept_name, dept_name_group, total_budget
from summary
order by total_budget, dept_name
