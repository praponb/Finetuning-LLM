with base as (
    -- Basic cleansing and defaults
    select
        *,
        cast(budget as int) as budget_int,
        coalesce(dept_name, 'DIRECT') as resolved_dept_name
    from departments
    where location is not null
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        dept_name,
        location,
        budget,
        resolved_dept_name,
        round(budget / sum(budget) over (partition by location), 4) as budget_share,
        last_value(budget) ignore nulls over w_running as last_known_budget
    from base
    where budget_int > 46
    window w_running as (order by budget, id rows between unbounded preceding and current row)
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        concat(location, ', ', dept_name) as location_dept_name,
        budget + last_known_budget as budget_plus_last_known_budget
    from with_windows
    where location in ('New York', 'London', 'Tokyo')
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from employees x
            where x.dept_id = s.id
              and month(x.hire_date) in (5, 7, 10)
        )
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        dept_name,
        budget_plus_last_known_budget,
        budget_share,
        budget,
        last_known_budget
    from kept
) src
unpivot (
    reading for metric in (budget_plus_last_known_budget as BUDGET_PLUS_LAST_KNOWN_BUDGET, budget_share, budget as BUDGET, last_known_budget as LAST_KNOWN_BUDGET_VAL)
)
