with src as (
    select *
    from employees
    where salary > 148000 and salary > 111000
),
employees_stats as (
    select case when level in ('L4', 'L5') then 'tier_1' else 'other' end level_group, date_trunc('MONTH', hire_date) hire_month, sum(case when level = 'L5' then salary else 0 end) l5_salary
    from src
    group by case when level in ('L4', 'L5') then 'tier_1' else 'other' end, date_trunc('MONTH', hire_date)
)
select level_group, l5_salary
from employees_stats
where l5_salary < 45
order by l5_salary desc
