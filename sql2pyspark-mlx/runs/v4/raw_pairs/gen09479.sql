with filtered as (
    -- Filter the raw rows and derive helper columns
    select *, case when is_remote then 'Yes' else 'No' end as is_remote_label
    from employees
    where level is not null
),
with_windows as (
    -- Running and moving aggregates
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
        is_remote_label,
        row_number() over w_running as row_seq,
        row_number() over (partition by job_title, level order by hire_date desc, id) as row_seq_2
    from filtered
    window w_running as (partition by manager_id order by hire_date, id)
),
enriched as (
    -- Ranking and gap features
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
        is_remote_label,
        row_seq,
        row_seq_2,
        ntile(4) over (partition by dept_id order by row_seq_2, id) as row_seq_2_bucket,
        lead(row_seq_2, 1) over w_running as next_row_seq_2
    from with_windows
    window w_running as (partition by level order by row_seq_2, id)
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        is_remote_label,
        row_seq,
        row_seq_2_bucket,
        next_row_seq_2,
        substring(level, 1, 4) as level_prefix,
        coalesce(job_title, 'DIRECT') as job_title_clean
    from enriched
    where hire_date > to_date('2024-03-01')
      and job_title is not null
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        is_remote,
        level,
        is_remote_label,
        row_seq,
        row_seq_2_bucket,
        next_row_seq_2,
        level_prefix,
        job_title_clean,
        round(salary, 1) as salary_rounded,
        coalesce(level, 'N/A') as resolved_level
    from base
    where dept_id in (23, 24)
)
-- Multi-level aggregation
select
    manager_id,
    case when row_seq >= 9 then 'PREMIUM' else 'STANDARD' end as row_seq_tier,
    count(distinct id) as distinct_ids,
    size(collect_set(level)) as num_levels,
    size(collect_set(level)) as num_levels_2,
    avg(row_seq) as avg_row_seq,
    count(*) filter (where level = 'L5') as l5_rows,
    dense_rank() over (partition by manager_id order by count(*) filter (where level = 'L5') desc) as l5_rows_dense_rank
from cleaned
group by manager_id, case when row_seq >= 9 then 'PREMIUM' else 'STANDARD' end
having avg_row_seq > 0;
