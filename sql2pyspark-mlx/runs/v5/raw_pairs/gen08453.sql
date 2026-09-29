with valid as (
    -- Basic cleansing and defaults
    select *, date_add(start_date, 41) as start_date_plus_41d
    from subscriptions
    where end_date > to_date('2023-11-03')
      and status = 'PAUSED'
)
-- Native SQL PIVOT
select *
from (
    select
        customer_id,
        status,
        monthly_fee
    from valid
) src
pivot (
    sum(monthly_fee) as total,
    count(1) as n
    for status in ('EXPIRED', 'ACTIVE')
);
