with valid as (
    select
        *,
        coalesce(job_title, 'Unknown') as job_title_filled,
        coalesce(level, 'UNKNOWN') as level_filled
    from employees
    where hire_date >= to_date('2023-12-01')
),
with_windows as (
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
        job_title_filled,
        level_filled,
        sum(salary) over (partition by job_title order by salary, id rows between unbounded preceding and current row) as cumulative_salary,
        row_number() over (partition by dept_id order by salary, id) as row_seq,
        round(salary / sum(salary) over (partition by manager_id, level), 4) as salary_share,
        rank() over w_recent as salary_rank
    from valid
    window w_recent as (partition by job_title, dept_id order by salary desc)
),
metrics as (
    select
        *,
        avg(salary_share) over (partition by level order by salary_rank, id rows between 6 preceding and current row) as salary_share_avg_6,
        cume_dist() over (order by cumulative_salary desc nulls first) as cumulative_salary_cume_dist
    from with_windows
),
with_windows_employees as (
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
        job_title_filled,
        level_filled,
        cumulative_salary,
        row_seq,
        salary_share,
        salary_rank,
        salary_share_avg_6,
        cumulative_salary_cume_dist,
        cume_dist() over (partition by level order by cumulative_salary_cume_dist desc) as cumulative_salary_cume_dist_cume_dist,
        avg(salary_share_avg_6) over (partition by level order by cumulative_salary_cume_dist, id rows between 5 preceding and 1 preceding) as salary_share_avg_6_avg_5_prior
    from metrics
)
select
    id,
    full_name,
    email,
    dept_id,
    manager_id,
    job_title,
    salary,
    level,
    job_title_filled
from with_windows_employees
order by manager_id nulls last;
