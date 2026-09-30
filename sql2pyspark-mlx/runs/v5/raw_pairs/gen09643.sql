with with_dims as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.paid_at,
        a.status,
        o.order_date
    from payments a
    left join orders o
        on a.order_id <=> o.id
    where o.store_id in (7, 14, 27)
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        paid_at,
        status
    from with_dims
    qualify rank() over (partition by order_id order by paid_at desc) = 1
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 7 days then 1 else 0 end as new_group_flag
    from ranked
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        paid_at,
        status,
        sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        date_format(paid_at, 'yyyy-MM-dd') as paid_at_label,
        round(burst_id, 1) as burst_id_rounded
    from sessionized
    where status in ('REFUNDED', 'SUCCESS')
)
select
    id,
    paid_at,
    status,
    burst_id
from valid
order by burst_id nulls last;
