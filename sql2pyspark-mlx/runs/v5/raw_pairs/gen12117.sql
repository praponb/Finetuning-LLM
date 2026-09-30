with base as (
    -- Keep recent, valid rows
    select *, coalesce(budget, 0) as budget_filled
    from departments
    where location is not null
      and budget > 1868000
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        location,
        budget_filled,
        budget
    from base
) src
unpivot include nulls (
    value for measure in (budget_filled as BUDGET_FILLED_VAL, budget as BUDGET_VAL)
)
