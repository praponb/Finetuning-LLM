with valid as (
    select
        *,
        upper(level) as level_upper,
        substring(email, 1, 4) as email_prefix
    from employees
    where job_title in ('Designer', 'Intern')
)
select
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    hire_date,
    is_remote,
    level,
    level_upper,
    email_prefix,
    avg(salary) over w_hist as salary_avg_2_prior,
    percent_rank() over w_hist_2 as salary_percent_rank,
    sum(salary) over (order by hire_date, id rows between unbounded preceding and current row) as cumulative_salary,
    lead(salary, 1) over (partition by level order by hire_date, id) as next_salary,
    count(salary) over (partition by level) as level_count_salary
from valid
window w_hist as (order by hire_date, id rows between 2 preceding and 1 preceding), w_hist_2 as (partition by dept_id, level order by salary desc);
