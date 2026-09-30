with remaining_keys as (
    select dept_name
    from departments
    intersect
    select dept_name
    from departments
    where dept_name = 'Legal'
),
kept_rows as (
    select s.*
    from departments s
    join remaining_keys k on s.dept_name = k.dept_name
),
cleaned as (
    select *, coalesce(budget, 0) as budget_filled
    from kept_rows
    where location is distinct from 'Berlin'
),
eligible as (
    select s.*
    from cleaned s
    where exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.full_name like 'Ja%'
        )
),
metrics as (
    select
        *,
        dense_rank() over (partition by location order by budget_filled desc) as budget_filled_dense_rank,
        rank() over (order by budget_filled nulls last) as budget_filled_rank
    from eligible
),
scored as (
    select
        id,
        dept_name,
        dense_rank() over w_recent as budget_filled_dense_rank_2,
        round(budget_filled_rank / sum(budget_filled_rank) over (partition by location), 4) as budget_filled_rank_share,
        stddev_samp(budget_filled_dense_rank) over (partition by dept_name) as dept_name_stddev_samp_budget_filled_dense_rank
    from metrics
    window w_recent as (order by budget_filled nulls last)
),
not_flagged as (
    select s.*
    from scored s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.dept_id in (11, 37, 40)
        )
),
scored_departments as (
    select
        *,
        round(budget_filled_dense_rank_2 / sum(budget_filled_dense_rank_2) over (partition by dept_name), 4) as budget_filled_dense_rank_2_share,
        stddev_samp(budget_filled_rank_share) over (partition by dept_name) as dept_name_stddev_samp_budget_filled_rank_share
    from not_flagged
),
stats as (
    select
        dept_name,
        max(budget_filled_dense_rank_2_share) filter (where dept_name = 'Engineering') as engineering_max_budget_filled_dense_rank_2_share,
        sum(case when dept_name = 'Engineering' then budget_filled_dense_rank_2 else 0 end) as engineering_budget_filled_dense_rank_2
    from scored_departments
    group by 1
)
select dept_name
from stats
