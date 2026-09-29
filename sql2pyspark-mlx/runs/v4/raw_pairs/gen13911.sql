with deduped as (
    select *
    from (
        select *, row_number() over (partition by level order by hire_date desc nulls first, id) as rn
        from employees
    ) ranked
    where rn = 1
),
enriched as (
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
        rn,
        last_value(level, true) over (partition by manager_id order by salary, id rows between unbounded preceding and current row) as last_known_level,
        lead(salary, 2) over (partition by manager_id, level order by hire_date, id) as next_salary,
        round(salary / sum(salary) over (partition by manager_id), 4) as salary_share,
        sum(salary) over (partition by level order by rn, id rows between unbounded preceding and current row) as cumulative_salary,
        sum(salary) over (order by salary, id rows between unbounded preceding and current row) as cumulative_salary_2
    from deduped
),
enriched_employees as (
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
        rn,
        last_known_level,
        next_salary,
        salary_share,
        cumulative_salary,
        cumulative_salary_2,
        rank() over (partition by level order by salary_share desc) as salary_share_rank,
        round(next_salary / sum(next_salary) over (partition by level), 4) as next_salary_share,
        lag(cumulative_salary, 1) over w_part as prev_cumulative_salary,
        lag(salary, 1) over w_part as prev_salary,
        ntile(4) over (partition by manager_id, job_title order by next_salary desc, id) as next_salary_bucket
    from enriched
    window w_part as (partition by level order by cumulative_salary, id)
)
select
    id,
    job_title,
    salary,
    is_remote,
    last_known_level,
    next_salary,
    salary_share,
    cumulative_salary,
    salary_share_rank
from enriched_employees
order by salary_share_rank desc
