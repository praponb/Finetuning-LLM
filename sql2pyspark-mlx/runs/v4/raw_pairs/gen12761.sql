with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        level,
        day(hire_date) as hire_date_day,
        coalesce(level, 'DIRECT') as level_filled
    from employees
    where job_title <> 'Director'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by job_title, dept_id order by hire_date desc, id) as seq_no,
        row_number() over (partition by manager_id, level order by hire_date desc, id) as event_seq
    from filtered
),
metrics as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by job_title order by hire_date_day, id) as seq_no_2,
        ntile(4) over (order by event_seq, id) as event_seq_bucket
    from with_windows
)
-- Ranking and gap features
select
    *,
    avg(event_seq_bucket) over (partition by job_title order by hire_date_day, id rows between 2 preceding and current row) as event_seq_bucket_avg_2,
    ntile(3) over (partition by job_title order by hire_date_day, id) as hire_date_day_bucket
from metrics;
