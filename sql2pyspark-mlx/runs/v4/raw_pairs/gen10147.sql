select
    job_title,
    round((100 * sum(case when job_title = 'Analyst' then 1 else 0 end)) / count(*), 1) as pct_analyst,
    concat_ws(', ', sort_array(collect_set(level))) as level_values,
    max(salary) - min(salary) as salary_range
from employees
where job_title in ('Manager', 'Designer', 'Intern') and level = 'L5'
group by job_title
