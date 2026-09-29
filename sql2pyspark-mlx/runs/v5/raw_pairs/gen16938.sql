with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        coalesce(budget, 0) budget_filled,
        trim(location) clean_location
    from departments
    where location = 'London'
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(budget) over (partition by location order by budget_filled, id rows between 6 preceding and current row) last7_sum_budget,
        avg(budget) over (partition by location order by budget_filled, id rows between 6 preceding and current row) last7_avg_budget,
        count(budget) over (partition by location order by budget_filled, id rows between 6 preceding and current row) last7_count_budget
    from cleaned
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and x.level not in ('L1', 'L5')
        )
),
enriched as (
    -- Ranking and gap features
    select
        id,
        budget,
        budget_filled,
        last7_sum_budget,
        last7_count_budget,
        lead(last7_sum_budget, 1, 0) over (order by budget, id) next_last7_sum_budget,
        ntile(3) over (order by last7_count_budget desc, id) last7_count_budget_bucket
    from kept
    where last7_count_budget = 12
)
select
    id,
    last7_sum_budget,
    next_last7_sum_budget,
    last7_count_budget_bucket
from enriched
order by last7_sum_budget desc nulls first
