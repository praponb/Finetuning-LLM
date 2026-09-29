with department_stats as (
    select dept_id, concat_ws(', ', sort_array(collect_set(job_title))) as job_title_values
    from employees
    group by dept_id
)
select departments.id, departments.dept_name, department_stats.job_title_values
from departments
left join department_stats on departments.id = department_stats.dept_id
order by id desc, job_title_values
