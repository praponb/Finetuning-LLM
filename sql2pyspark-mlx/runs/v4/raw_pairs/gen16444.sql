with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 7 days then 1 else 0 end as new_group_flag
    from payments
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        method,
        amount,
        new_group_flag,
        island_id,
        date_format(paid_at, 'MMM yyyy') as paid_at_label,
        coalesce(method, 'OTHER') as method_filled
    from sessions_payments
    where paid_at > to_timestamp('2023-12-27 21:00:00')
      and method is not null
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by order_id, method order by new_group_flag nulls first, id) as rank_in_group
        from base
    ) ranked
    where rank_in_group = 1
)
select
    id,
    order_id,
    method,
    amount,
    new_group_flag,
    paid_at_label,
    method_filled,
    rank_in_group,
    case when method rlike '^(bank_transfer|card)$' then 'OTHER_ZONE' when method rlike '^(paypal|cash)$' then 'EXTENDED' else 'CORE' end as method_zone
from ranked;
