with base as (
    -- Keep recent, valid rows
    select
        *,
        round(salary, 2) as salary_rounded,
        coalesce(job_title, 'N/A') as resolved_job_title
    from employees
    where hire_date >= date '2023-03-16'
      and email is not null
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by job_title order by salary, id) as rn
    from base
    qualify rn = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        round(salary / sum(salary) over (partition by level), 4) as salary_share,
        dense_rank() over (partition by manager_id order by salary desc) as salary_dense_rank,
        first_value(job_title, true) over (partition by manager_id order by hire_date, id rows between unbounded preceding and current row) as first_known_job_title,
        percent_rank() over (partition by level order by salary_rounded desc) as salary_rounded_percent_rank,
        ntile(4) over w_running as salary_rounded_bucket
    from ranked
    window w_running as (partition by dept_id order by salary_rounded desc, id)
),
with_dims as (
    -- Attach departments attributes
    select
        s.id,
        s.dept_id,
        s.job_title,
        s.is_remote,
        s.level,
        s.salary_rounded,
        s.resolved_job_title,
        s.salary_dense_rank,
        s.salary_rounded_percent_rank,
        s.salary_rounded_bucket,
        d.budget,
        d.dept_name
    from with_windows s
    left join departments d
        on s.dept_id = d.id
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    dept_id,
    measure,
    value
from (
    select
        id,
        dept_id,
        salary_rounded,
        budget,
        salary_rounded_percent_rank
    from with_dims
) src
unpivot (
    value for measure in (salary_rounded as SALARY_ROUNDED_VAL, budget as BUDGET, salary_rounded_percent_rank as SALARY_ROUNDED_PERCENT_RANK)
);
