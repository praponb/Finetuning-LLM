with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        floor(cost) as cost_whole,
        add_months(shipped_date, 1) as shipped_date_plus_months
    from shipments
    where delivered_date > date '2023-06-04'
      and carrier is distinct from 'Flash'
),
latest as (
    -- QUALIFY-style window filter
    select *
    from filtered
    qualify row_number() over (partition by status, order_id order by shipped_date desc, id) = 1
),
joined as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.carrier,
        a.cost,
        a.status,
        a.cost_whole,
        a.shipped_date_plus_months,
        b.order_date
    from latest a
    left join orders b
        on a.order_id = b.id
    where b.order_date between '2023-06-07' and '2025-04-13'
),
joined_shipments as (
    -- Attach orders attributes
    select
        a.*,
        d.discount,
        d.shipping_fee
    from joined a
    left join orders d
        on a.order_id = d.id
),
enriched as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.cost,
        s.status,
        s.cost_whole,
        s.discount,
        s.shipping_fee,
        b.status as order_status,
        b.channel,
        b.discount as order_discount
    from joined_shipments s
    left join orders b
        on s.order_id <=> b.id
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        order_id,
        shipping_fee,
        order_discount,
        cost
    from enriched
) src
unpivot (
    metric_value for metric_name in (shipping_fee as SHIPPING_FEE_VAL, order_discount as ORDER_DISCOUNT_VAL, cost)
)
