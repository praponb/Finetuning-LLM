with valid as (
    -- Basic cleansing and defaults
    select *, lower(job_title) as job_title_lower
    from employees
    where job_title is distinct from 'Director'
      and level <> 'L4'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        lead(salary, 1) over (partition by manager_id order by salary, id) as next_salary,
        sum(salary) over (partition by level, job_title order by hire_date, id rows between unbounded preceding and current row) as running_salary,
        dense_rank() over (partition by level, manager_id order by salary desc) as salary_dense_rank,
        last_value(job_title) ignore nulls over (partition by level order by salary, id rows between unbounded preceding and current row) as last_known_job_title
    from valid
    where level is null
),
enriched_employees as (
    -- Ranking and gap features
    select
        *,
        avg(salary) over w_grp as salary_avg_6,
        ntile(5) over (partition by level order by salary_dense_rank desc, id) as salary_dense_rank_bucket
    from enriched
    window w_grp as (partition by job_title order by salary, id rows between 6 preceding and current row)
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by job_title order by salary_dense_rank desc, id) as rank_in_group
    from enriched_employees
    qualify rank_in_group = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by level order by salary, id) as row_num
    from deduped
    qualify row_num = 1
)
-- Aggregate with conditional metrics
select
    job_title,
    case when running_salary >= 641421 then 'LARGE' else 'SMALL' end as running_salary_tier,
    round(avg(salary_dense_rank), 2) as avg_salary_dense_rank,
    round(avg(running_salary), 2) as avg_running_salary,
    avg(salary_dense_rank) filter (where level = 'L2') as l2_avg_salary_dense_rank
from ranked
group by 1, 2
order by avg_running_salary desc
