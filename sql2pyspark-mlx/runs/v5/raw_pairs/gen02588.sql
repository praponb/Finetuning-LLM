with enriched as (
    -- Window metrics over several different partitions
    select
        id,
        budget,
        avg(budget) over (partition by location order by budget, id rows between 3 preceding and current row) as budget_avg_3,
        first_value(budget, true) over (partition by location order by budget, id rows between unbounded preceding and current row) as first_known_budget
    from departments
),
base as (
    -- Basic cleansing and defaults
    select *, first_known_budget * budget as first_known_budget_times_budget
    from enriched
    where budget_avg_3 between 2551548 and 3022884
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        budget,
        first_known_budget_times_budget,
        ntile(10) over (order by first_known_budget desc, id) as first_known_budget_bucket,
        row_number() over (order by budget, id) as row_seq
    from base
),
latest_match as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.budget,
        s.first_known_budget_times_budget,
        x.employee_level
    from with_windows s
    left join lateral (
            select c.level as employee_level
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) x on true
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.job_title is null
        )
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by employee_level order by budget desc, id) as row_num
    from eligible
    qualify row_num = 1
),
totals as (
    -- Grouped metrics
    select
        case when first_known_budget_times_budget >= 81 then 'LARGE' else 'SMALL' end as first_known_budget_times_budget_tier,
        avg(budget) filter (where employee_level = 'L5') as l5_avg_budget,
        avg(first_known_budget_times_budget) filter (where employee_level = 'L5') as l5_avg_first_known_budget_times_budget,
        sort_array(collect_set(employee_level)) as employee_level_list,
        count(*) filter (where employee_level = 'L1') as l1_rows,
        sum(case when employee_level = 'L3' then first_known_budget_times_budget else 0 end) as l3_first_known_budget_times_budget,
        count_if(employee_level = 'L1') as l1_events
    from latest
    group by case when first_known_budget_times_budget >= 81 then 'LARGE' else 'SMALL' end
)
select
    first_known_budget_times_budget_tier,
    employee_level_list,
    l1_rows,
    l3_first_known_budget_times_budget,
    l1_events
from totals
order by l3_first_known_budget_times_budget nulls first;
