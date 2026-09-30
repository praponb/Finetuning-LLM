with deduped as (
    -- QUALIFY-style window filter
    select *
    from employees
    qualify row_number() over (partition by manager_id order by hire_date desc, id) = 1
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        add_months(hire_date, 3) as hire_date_plus_months,
        coalesce(level, 'N/A') as resolved_level
    from deduped
    where hire_date >= date '2023-06-19'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        full_name,
        dept_id,
        job_title,
        hire_date,
        hire_date_plus_months,
        resolved_level,
        lead(salary) over w_hist as next_salary,
        lag(salary) over w_hist as prev_salary,
        row_number() over w_hist as seq_no
    from valid
    window w_hist as (partition by manager_id, job_title order by hire_date_plus_months, id)
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        hire_date_plus_months,
        resolved_level,
        next_salary,
        prev_salary,
        seq_no
    from (
        select *, row_number() over (partition by dept_id, job_title order by hire_date_plus_months, id) as row_num
        from enriched
    ) t
    where row_num = 1
),
enriched_employees as (
    -- Window metrics over several different partitions
    select
        *,
        avg(prev_salary) over (order by next_salary, id rows between 2 preceding and current row) as prev_salary_avg_2,
        sum(seq_no) over (order by next_salary, id rows between unbounded preceding and current row) as running_seq_no,
        ntile(4) over (order by next_salary, id) as next_salary_bucket
    from latest
)
select
    id,
    hire_date_plus_months,
    next_salary,
    prev_salary,
    prev_salary_avg_2
from enriched_employees
order by hire_date_plus_months nulls first
