with RecentEmployees as (
    select *
    from employees
    where hire_date between '2024-01-23' and '2025-04-01' and email like '%@outlook.com' and manager_id = 39
),
agg as (
    select
        case when salary >= 135000 then 'high' when salary >= 85000 then 'mid' else 'low' end as salary_bucket,
        is_remote,
        count(case when level = 'L3' then 1 end) as l3_count,
        max(salary) - min(salary) as salary_range
    from RecentEmployees
    group by case when salary >= 135000 then 'high' when salary >= 85000 then 'mid' else 'low' end, is_remote
)
select
    salary_bucket,
    is_remote,
    l3_count,
    salary_range
from agg
where l3_count > 9;
