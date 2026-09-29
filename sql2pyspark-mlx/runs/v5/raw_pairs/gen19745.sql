with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        dept_name,
        rank() over w_hist as budget_rank,
        ntile(5) over w_grp as budget_bucket
    from departments
    window w_hist as (partition by location, dept_name order by budget desc), w_grp as (partition by dept_name order by budget, id)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(budget_bucket as string) as budget_bucket_num,
        round(budget_rank, 2) as budget_rank_rounded
    from with_windows
    where dept_name is distinct from 'Marketing'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        budget_rank,
        budget_bucket_num,
        budget_rank_rounded,
        case when budget_rank >= 7 then 'High' when budget_rank >= 4 then 'Medium' else 'Low' end as budget_rank_band,
        initcap(trim(dept_name)) as dept_name_title
    from valid
    where dept_name is not null
      and dept_name is distinct from 'HR'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(budget_rank_rounded) over w_ordered as running_budget_rank_rounded,
        ntile(5) over (order by budget_rank desc, id) as budget_rank_bucket
    from cleaned
    window w_ordered as (order by budget_rank, id rows between unbounded preceding and current row)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lag(running_budget_rank_rounded) over (order by budget_rank_rounded, id) as prev_running_budget_rank_rounded,
        lead(running_budget_rank_rounded) over (order by budget_rank_rounded, id) as next_running_budget_rank_rounded,
        lag(budget_rank_rounded) over (order by budget_rank_rounded, id) as prev_budget_rank_rounded,
        lead(budget_rank_rounded) over (order by budget_rank_rounded, id) as next_budget_rank_rounded,
        row_number() over (order by budget_rank_rounded, id) as row_seq,
        lead(budget_rank_bucket, 1, 0) over (order by running_budget_rank_rounded, id) as next_budget_rank_bucket
    from enriched
)
select
    id,
    budget_rank,
    budget_rank_rounded,
    budget_rank_band,
    next_running_budget_rank_rounded,
    next_budget_rank_rounded,
    next_budget_rank_bucket
from metrics;
