with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(amount, 2) as amount_rounded,
        cast(shipping_fee as int) as shipping_fee_int,
        coalesce(channel, 'NONE') as channel_filled
    from orders
    where channel is distinct from 'web'
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, x.unit_price
    from cleaned s
    left join lateral (
            select c.unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
scored as (
    -- Ranking and gap features
    select
        *,
        min(discount) over w_running as last4_min_discount,
        max(discount) over w_running as last4_max_discount,
        count(discount) over w_running as last4_count_discount,
        avg(discount) over w_running as last4_avg_discount
    from latest_match
    where shipping_fee is null
    window w_running as (partition by status order by order_date, id rows between 3 preceding and current row)
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.order_date,
        s.amount,
        s.discount,
        s.shipping_fee_int,
        s.last4_min_discount,
        s.last4_max_discount,
        s.last4_count_discount,
        s.last4_avg_discount,
        top1.quantity
    from scored s
    join lateral (
            select c.quantity
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
)
select
    id,
    customer_id,
    order_date,
    discount,
    last4_min_discount,
    last4_count_discount,
    last4_avg_discount
from with_latest
order by id desc
limit 5;
