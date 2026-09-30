with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        round(amount, 2) as amount_rounded,
        length(method) as method_length
    from payments
    where order_id is not null
)
-- Multi-level aggregation
select
    date_trunc('month', paid_at) as activity_month,
    status,
    sum(case when method = 'cash' then amount_rounded else 0 end) as cash_amount_rounded,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    count(distinct method) as distinct_methods,
    grouping_id() as grouping_id
from base
group by grouping sets ((date_trunc('month', paid_at), status), (date_trunc('month', paid_at)))
order by cash_amount_rounded nulls first
