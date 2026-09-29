with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        level,
        ceil(salary) as salary_whole
    from employees
    where hire_date >= to_date('2023-02-18')
      and hire_date < '2025-02-01'
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        job_title,
        salary,
        level,
        row_number() over w_grp as row_seq,
        row_number() over w_recent as event_seq,
        avg(salary) over (partition by manager_id order by salary_whole, id rows between 5 preceding and current row) as salary_avg_5
    from cleaned
    window w_grp as (order by hire_date, id), w_recent as (partition by level order by salary desc, id)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by job_title, level order by row_seq desc nulls first, id) as row_num
        from with_windows
    ) t
    where row_num = 1
),
scored as (
    -- Running and moving aggregates
    select
        id,
        job_title,
        level,
        row_seq,
        event_seq,
        lag(salary) over w_recent as prev_salary,
        lead(salary) over w_recent as next_salary,
        lag(salary_avg_5) over w_recent as prev_salary_avg_5,
        lead(salary_avg_5) over w_recent as next_salary_avg_5,
        row_number() over w_recent as row_seq_2,
        first_value(event_seq, true) over (partition by job_title order by row_seq, id rows between unbounded preceding and current row) as first_known_event_seq
    from latest
    window w_recent as (partition by level order by salary_avg_5, id)
)
-- Aggregate with conditional metrics
select
    level,
    count(*) as event_count,
    size(collect_set(level)) as num_levels
from scored
group by level
order by event_count nulls last;
