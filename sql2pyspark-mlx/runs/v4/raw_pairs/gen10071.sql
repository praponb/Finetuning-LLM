with base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        length(plan) as plan_length
    from subscriptions
    where end_date >= '2023-12-27'
)
-- Native SQL PIVOT
select *
from (
    select
        customer_id,
        plan,
        status,
        monthly_fee
    from base
) src
pivot (
    avg(monthly_fee)
    for status in ('CANCELLED', 'PAUSED')
)
