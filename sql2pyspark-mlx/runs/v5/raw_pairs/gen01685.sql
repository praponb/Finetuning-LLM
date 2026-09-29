with with_windows as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        job_title,
        salary,
        hire_date,
        is_remote,
        lead(salary) over w_part as next_salary,
        lag(salary) over w_part as prev_salary,
        row_number() over w_part as row_seq,
        sum(salary) over (partition by job_title, dept_id order by hire_date, id rows between unbounded preceding and current row) as running_salary
    from employees
    window w_part as (partition by dept_id order by hire_date, id)
),
matched_keys as (
    -- Set intersection on keys
    select id, job_title
    from with_windows
    intersect all
    select id, job_title
    from employees
    where full_name like '%Hen%'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    join matched_keys k on s.id = k.id and s.job_title = k.job_title
),
base as (
    -- Basic cleansing and defaults
    select *, cast(row_seq as double) as row_seq_str
    from reconciled
    where hire_date >= date '2023-02-13'
      and row_seq > 5
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by row_seq desc) as row_seq_rank,
        avg(row_seq) over (partition by job_title) as job_title_avg_row_seq,
        cume_dist() over (order by running_salary desc nulls last) as running_salary_cume_dist
    from base
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        prev_salary,
        dense_rank() over (order by salary desc) as salary_dense_rank,
        round(row_seq / sum(row_seq) over (partition by job_title), 4) as row_seq_share,
        rank() over (order by running_salary nulls last) as running_salary_rank
    from metrics
)
select
    full_name,
    email,
    row_seq_share,
    running_salary_rank
from enriched
order by row_seq_share, running_salary_rank
