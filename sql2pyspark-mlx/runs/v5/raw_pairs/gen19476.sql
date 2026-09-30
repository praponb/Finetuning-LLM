with filtered as (
    -- Basic cleansing and defaults
    select *, case when budget >= 3097000 then 'High' when budget >= 1883000 then 'Medium' else 'Low' end budget_tier
    from departments
    where dept_name = 'Marketing'
),
enriched as (
    -- Ranking and gap features
    select
        id,
        budget,
        budget_tier,
        max(budget) over (partition by dept_name) dept_name_max_budget,
        sum(budget) over (partition by location order by budget, id rows between unbounded preceding and current row) cumulative_budget
    from filtered
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        budget,
        budget_tier,
        dept_name_max_budget,
        cumulative_budget,
        lead(budget) over (order by budget, id) next_budget,
        lag(cumulative_budget) over (order by budget, id) prev_cumulative_budget,
        lead(cumulative_budget) over (order by budget, id) next_cumulative_budget,
        row_number() over (order by budget, id) seq_no,
        lead(budget, 1) over (order by dept_name_max_budget, id) next_budget_2
    from enriched
    where dept_name_max_budget < 2451854
),
matched_keys as (
    -- Set difference on keys
    select id
    from with_windows
    except
    select id
    from departments
    where budget <= 2724000
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join matched_keys k on s.id = k.id
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.level not in ('L1', 'L3')
        )
)
-- Aggregate with conditional metrics
select
    case when next_budget_2 >= 3906000 then 'PREMIUM' else 'STANDARD' end as next_budget_2_tier,
    case when seq_no >= 8 then 'HIGH' else 'LOW' end as seq_no_tier,
    percentile_approx(next_budget_2, 0.5) as next_budget_2_p50,
    percentile_approx(next_cumulative_budget, 0.9) as next_cumulative_budget_p90,
    round(avg(prev_cumulative_budget), 2) as avg_prev_cumulative_budget,
    grouping(case when next_budget_2 >= 3906000 then 'PREMIUM' else 'STANDARD' end) as grouping_level
from kept
group by grouping sets ((case when next_budget_2 >= 3906000 then 'PREMIUM' else 'STANDARD' end, case when seq_no >= 8 then 'HIGH' else 'LOW' end), (case when next_budget_2 >= 3906000 then 'PREMIUM' else 'STANDARD' end), ())
