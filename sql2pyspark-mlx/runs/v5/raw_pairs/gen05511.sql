with eligible as (
    select s.*
    from orders s
    where not exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.quantity > 3
        )
),
eligible_orders as (
    select s.*
    from eligible s
    where exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.unit_price < 190
        )
),
deduped as (
    select *
    from (
        select *, rank() over (partition by store_id order by shipping_fee desc) as rn
        from eligible_orders
    ) ranked
    where rn = 1
),
filtered as (
    select
        *,
        case when channel in ('web', 'store') then 'Core' else 'Other' end as channel_group,
        discount - shipping_fee as discount_minus_shipping_fee
    from deduped
    where channel is not null
      and channel is distinct from 'store'
),
not_flagged as (
    select s.*
    from filtered s
    where not exists (
            select 1
            from shipments ch
            where ch.order_id = s.id
              and ch.shipped_date <= s.order_date
        )
),
eligible_orders_2 as (
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from shipments x
            where x.order_id = s.id
              and x.delivered_date <= s.order_date
        )
),
with_last as (
    select
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.channel,
        s.discount,
        s.shipping_fee,
        s.rn,
        s.channel_group,
        top1.payment_status,
        top1.payment_amount
    from eligible_orders_2 s
    join lateral (
            select c.status as payment_status, c.amount as payment_amount
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
)
select *
from (
    select
        store_id,
        channel,
        payment_amount
    from with_last
) src
pivot (
    max(payment_amount)
    for channel in ('web', 'mobile')
)
