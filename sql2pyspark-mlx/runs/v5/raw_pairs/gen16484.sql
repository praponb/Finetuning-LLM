select
    level,
    job_title,
    max(hire_date) last_hire_date,
    count(distinct salary) unique_levels
from employees
where dept_id = 13 and level in ('L1', 'L4', 'L3')
group by level, job_title
having unique_levels > 5
order by level desc
