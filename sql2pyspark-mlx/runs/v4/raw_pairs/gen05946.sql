with valid as (
    -- Filter the raw rows and derive helper columns
    select *, round(shipping_fee * 100, 2) as adjusted_shipping_fee
    from orders
    where order_date > to_date('2024-03-15')
),
enriched as (
    -- Attach stores attributes
    select e.*, d.opened_date
    from valid e
    inner join stores d
        on e.store_id = d.id
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by channel order by adjusted_shipping_fee desc, id asc) as row_num
    from enriched
    qualify row_num = 1
),
enriched_orders as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(4) over (partition by store_id order by amount asc, id asc) as amount_bucket,
        avg(shipping_fee) over (order by shipping_fee asc, id asc rows between 6 preceding and 1 preceding) as shipping_fee_avg_6_prior,
        lead(amount, 1) over (partition by channel, store_id order by discount asc, id asc) as next_amount,
        avg(shipping_fee) over (partition by store_id order by order_date asc, id asc rows between 2 preceding and current row) as shipping_fee_avg_2
    from deduped
),
with_last as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, top1.order_item_unit_price
    from enriched_orders s
    left join lateral (
            select c.unit_price as order_item_unit_price
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
)
select
    s.id,
    s.customer_id,
    s.order_date,
    s.channel,
    s.amount,
    s.shipping_fee,
    s.adjusted_shipping_fee,
    s.row_num,
    s.amount_bucket,
    case when channel rlike '^(web|mobile)$' then 'PRIORITY' when channel rlike '^(store|partner)$' then 'CORE' else 'EXTENDED' end as channel_zone,
    (select sum(t2.monthly_fee) from subscriptions t2 where t2.customer_id = s.customer_id) as sum_subscription_monthly_fee
from with_last s;
