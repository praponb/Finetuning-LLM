select *
from (
    select level, avg(salary) as avg_salary
    from employees
    group by level
) as agg
where avg_salary < 67000
order by avg_salary desc
