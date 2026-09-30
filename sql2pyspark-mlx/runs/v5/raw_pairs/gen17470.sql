with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        order_date,
        status,
        channel,
        shipping_fee,
        round(shipping_fee, 2) as shipping_fee_rounded,
        shipping_fee * amount as amount_with_shipping,
        coalesce(channel, 'OTHER') as channel_filled
    from orders
    where order_date > date '2023-07-15'
      and discount < 0.31
),
matched_keys as (
    -- Set intersection on keys
    select status, id
    from cleaned
    intersect
    select status, id
    from orders
    where discount < 0.24
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    join matched_keys k on s.status = k.status and s.id = k.id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept_rows s
    where not exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.paid_at < s.order_date
        )
),
metrics as (
    -- Running and moving aggregates
    select
        store_id,
        status,
        shipping_fee_rounded,
        cume_dist() over (partition by store_id order by shipping_fee desc) as shipping_fee_cume_dist,
        sum(amount_with_shipping) over w_ordered as status_sum_amount_with_shipping,
        rank() over (partition by status order by shipping_fee_rounded desc nulls last) as shipping_fee_rounded_rank,
        avg(shipping_fee_rounded) over (partition by store_id) as store_id_avg_shipping_fee_rounded,
        cume_dist() over w_running as shipping_fee_cume_dist_2
    from eligible
    window w_ordered as (partition by status, channel), w_running as (partition by status order by shipping_fee desc nulls first)
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            store_id,
            status,
            shipping_fee_cume_dist_2
        from metrics
    ) src
    pivot (
        sum(shipping_fee_cume_dist_2) as value_sum,
        count(1) as item_count
        for status in ('SHIPPED' as shipped, 'CANCELLED' as cancelled)
    )
)
select
    s.store_id,
    s.shipped_value_sum,
    s.shipped_item_count,
    s.cancelled_item_count,
    (select max(t2.revenue) from store_sales t2 where t2.store_id = s.store_id) as max_store_sale_revenue
from wide s
order by store_id desc nulls last
limit 20;
