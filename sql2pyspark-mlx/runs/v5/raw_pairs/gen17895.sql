with filtered as (
    -- Keep recent, valid rows
    select *, substring(dept_name, 1, 4) dept_name_prefix
    from departments
    where dept_name is distinct from 'HR'
      and location = 'Chiang Mai'
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by location order by budget nulls first, id) as latest_rank
    from filtered
    qualify latest_rank = 1
),
scored as (
    -- Running and moving aggregates
    select
        id,
        location,
        latest_rank,
        count(budget) over w_hist last4_count_budget,
        min(budget) over w_hist last4_min_budget,
        avg(budget) over w_hist last4_avg_budget,
        sum(budget) over w_hist_2 cumulative_budget
    from latest
    where budget <= 2162000
    window w_hist as (partition by dept_name order by latest_rank, id rows between 3 preceding and current row), w_hist_2 as (partition by location order by latest_rank, id rows between unbounded preceding and current row)
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from scored
    qualify row_number() over (partition by location order by last4_count_budget desc nulls last, id) = 1
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from deduped s
    where exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.level in ('L5', 'L1')
        )
)
select
    id,
    latest_rank,
    last4_min_budget,
    last4_avg_budget,
    cumulative_budget
from kept;
