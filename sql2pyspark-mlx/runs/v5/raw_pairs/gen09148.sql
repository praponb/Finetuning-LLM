with filtered as (
    -- Keep recent, valid rows
    select *, trim(carrier) as clean_carrier
    from shipments
    where delivered_date >= to_date('2023-02-24')
      and order_id is null
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        order_id,
        shipped_date,
        cost
    from (
        select *, row_number() over (partition by carrier order by shipped_date nulls last, id) as latest_rank
        from filtered
    ) ranked
    where latest_rank = 1
),
with_dims as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        p.status
    from deduped a
    left join orders p
        on a.order_id = p.id
),
enriched as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        d.channel
    from with_dims s
    left join orders d
        on s.order_id = d.id
),
with_dims_shipments as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        b.discount,
        b.amount,
        b.channel as order_channel
    from enriched src
    join orders b
        on src.order_id = b.id
),
enriched_shipments as (
    -- Attach orders attributes
    select
        s.*,
        o.amount as order_amount,
        o.channel
    from with_dims_shipments s
    join orders o
        on s.order_id = o.id
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        order_id,
        discount,
        amount,
        order_channel,
        channel,
        ntile(4) over (partition by channel order by amount desc, id) as amount_bucket,
        sum(order_amount) over (partition by order_id order by amount, id rows between unbounded preceding and current row) as cumulative_order_amount,
        sum(amount) over (partition by order_id order by amount, id rows between unbounded preceding and current row) as running_amount,
        ntile(5) over (partition by order_channel order by amount desc, id) as amount_bucket_2
    from enriched_shipments
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        channel,
        discount,
        amount
    from with_windows
) src
unpivot (
    val for metric_name in (discount as DISCOUNT_VAL, amount as AMOUNT_VAL)
) unp
