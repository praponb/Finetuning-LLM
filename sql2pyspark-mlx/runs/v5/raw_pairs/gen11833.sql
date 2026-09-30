select level, total_salary, pct_l1
from (
    select level, sum(salary) as total_salary, round((100 * sum(case when level = 'L1' then 1 else 0 end)) / count(*), 1) as pct_l1
    from employees
    group by level
) as sub
where pct_l1 > 61
order by total_salary desc;
