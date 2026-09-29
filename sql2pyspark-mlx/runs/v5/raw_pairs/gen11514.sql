with enriched as (
    select e.*, b.hire_date as employee_hire_date
    from employees e
    left join employees b
        on e.manager_id = b.id
)
select
    case when salary >= 134000 then 'LARGE' else 'SMALL' end as salary_tier,
    avg(salary) filter (where job_title = 'Analyst') as analyst_avg_salary,
    sum(salary) as total_salary,
    round(avg(salary), 2) as avg_salary,
    round(sum(salary) / nullif(count(case when level = 'L3' then 1 end), 0), 2) as salary_per_l3,
    count(distinct level) as distinct_levels
from enriched
group by case when salary >= 134000 then 'LARGE' else 'SMALL' end;
