with cleaned as (
    select *, cast(salary as int) as salary_int
    from employees
    where manager_id is not null
)
select
    date_trunc('month', hire_date) as activity_month,
    date_trunc('week', hire_date) as week_start,
    sum(salary_int) filter (where job_title = 'Designer') as designer_total_salary_int,
    sum(salary) filter (where job_title = 'Analyst') as analyst_total_salary,
    count_if(level = 'L2') as l2_events,
    count(distinct dept_id) as distinct_dept_ids,
    count_if(job_title = 'Manager') as manager_events,
    approx_percentile(salary, 0.9) as salary_p90
from cleaned
group by activity_month, week_start
having analyst_total_salary < 0
