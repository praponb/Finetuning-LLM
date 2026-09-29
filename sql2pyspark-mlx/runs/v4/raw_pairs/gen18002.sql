with filtered as (
    select
        id,
        channel,
        discount,
        shipping_fee,
        coalesce(discount, 0) discount_filled,
        coalesce(channel, 'N/A') resolved_channel
    from orders
    where channel is not null
      and amount < 410
),
kept as (
    select s.*
    from filtered s
    where exists (
            select 1
            from order_items c
            where c.order_id = s.id
              and c.quantity < 6
        )
),
remaining_keys as (
    select channel, id
    from kept
    except
    select channel, id
    from orders
    where discount <= 0.17
),
reconciled as (
    select s.*
    from kept s
    inner join remaining_keys k on s.channel = k.channel and s.id = k.id
)
select
    id,
    channel,
    discount,
    shipping_fee,
    discount_filled,
    resolved_channel
from reconciled
order by channel desc nulls first, discount_filled
