with enriched as (
    -- Window metrics over several different partitions
    select
        *,
        lead(budget, 1) over (partition by dept_name order by budget, id) as next_budget,
        ntile(4) over (partition by location, dept_name order by budget desc, id) as budget_bucket,
        avg(budget) over (order by budget, id rows between 3 preceding and 1 preceding) as budget_avg_3_prior,
        lead(budget, 2) over w_ordered as next_budget_2,
        max(budget) over w_running as dept_name_max_budget
    from departments
    window w_ordered as (partition by location order by budget, id), w_running as (partition by dept_name)
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(next_budget_2 / sum(next_budget_2) over (partition by location), 4) as next_budget_2_share,
        avg(next_budget_2) over (partition by dept_name, location order by next_budget_2, id rows between 5 preceding and 1 preceding) as next_budget_2_avg_5_prior,
        percent_rank() over (partition by dept_name order by dept_name_max_budget nulls first) as dept_name_max_budget_percent_rank
    from enriched
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from scored
    qualify row_number() over (partition by location order by budget desc, id) = 1
)
-- Native SQL PIVOT
select *
from (
    select
        location,
        dept_name,
        budget_avg_3_prior
    from ranked
) src
pivot (
    avg(budget_avg_3_prior) as value_sum,
    count(1) as n
    for dept_name in ('Marketing' as marketing, 'Finance' as finance, 'Legal' as legal)
);
