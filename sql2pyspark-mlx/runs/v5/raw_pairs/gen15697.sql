select
    is_remote,
    dept_id,
    round((100 * sum(case when job_title = 'Director' then 1 else 0 end)) / count(*), 1) as pct_director,
    concat_ws(', ', sort_array(collect_set(level))) as level_values
from employees
group by is_remote, dept_id
having round((100 * sum(case when job_title = 'Director' then 1 else 0 end)) / count(*), 1) > 78
