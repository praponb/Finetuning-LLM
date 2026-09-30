with deduped as (
    select *
    from (
        select *, row_number() over (partition by channel, store_id order by shipping_fee desc nulls first, id) as rn
        from orders
    ) t
    where rn = 1
),
with_latest as (
    select
        s.id,
        s.customer_id,
        s.status,
        s.channel,
        s.discount,
        s.shipping_fee,
        s.rn,
        top1.order_item_unit_price
    from deduped s
    join lateral (
            select c.unit_price as order_item_unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    select s.*
    from with_latest s
    where not exists (
            select 1
            from shipments ch
            where ch.order_id = s.id
              and ch.status = 'IN_TRANSIT'
        )
),
with_dims as (
    select
        src.*,
        c.name,
        c.city
    from not_flagged src
    join customers c
        on src.customer_id <=> c.id
),
not_flagged_orders as (
    select s.*
    from with_dims s
    where not exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.amount >= 820
        )
)
select
    s.id,
    s.customer_id,
    s.channel,
    s.discount,
    s.shipping_fee,
    s.rn,
    s.name,
    (select min(a2.balance) from accounts a2 where a2.customer_id = s.customer_id) as min_account_balance
from not_flagged_orders s
order by customer_id desc nulls first
