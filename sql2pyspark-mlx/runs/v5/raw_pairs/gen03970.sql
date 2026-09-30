with deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by level order by hire_date desc nulls first, id) as rank_in_group
    from employees
    qualify rank_in_group = 1
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by job_title order by hire_date desc, id) as rank_in_group_2
    from deduped
    qualify rank_in_group_2 = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        avg(salary) over (order by hire_date, id rows between 5 preceding and 1 preceding) as salary_avg_5_prior,
        row_number() over (partition by manager_id order by rank_in_group, id) as row_seq,
        row_number() over (partition by manager_id order by salary, id) as row_seq_2
    from latest
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        manager_id,
        job_title,
        is_remote,
        salary_avg_5_prior,
        row_seq_2,
        row_number() over w_grp as row_seq_3,
        row_number() over w_grp as event_seq
    from metrics
    where manager_id is null
    window w_grp as (partition by job_title order by hire_date, id)
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        manager_id,
        job_title,
        salary_avg_5_prior,
        row_seq_3,
        event_seq,
        lag(row_seq_2) over (partition by manager_id order by salary_avg_5_prior, id) as prev_row_seq_2,
        lead(row_seq_2) over (partition by manager_id order by salary_avg_5_prior, id) as next_row_seq_2,
        lead(salary_avg_5_prior) over (partition by manager_id order by salary_avg_5_prior, id) as next_salary_avg_5_prior,
        lag(salary_avg_5_prior) over (partition by manager_id order by salary_avg_5_prior, id) as prev_salary_avg_5_prior,
        row_number() over (partition by manager_id order by salary_avg_5_prior, id) as event_seq_2
    from with_windows
),
filtered as (
    -- Keep recent, valid rows
    select *, substring(job_title, 1, 4) as job_title_prefix
    from enriched
    where full_name like 'O%'
)
select
    id,
    full_name,
    manager_id,
    job_title,
    row_seq_3,
    event_seq,
    next_row_seq_2,
    next_salary_avg_5_prior
from filtered
order by id nulls first;
