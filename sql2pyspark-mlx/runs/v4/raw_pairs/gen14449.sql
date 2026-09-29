with with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(amount) over (order by amount, id rows between unbounded preceding and current row) as cumulative_amount,
        sum(amount) over (partition by customer_id order by order_date, id rows between unbounded preceding and current row) as running_amount,
        dense_rank() over (order by amount nulls last) as amount_dense_rank
    from orders
),
with_last as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select s.*, x.order_item_quantity
    from with_windows s
    join lateral (
            select c.quantity as order_item_quantity
            from order_items c
            where c.order_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_last s
    where not exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.product_id = 18
        )
),
summary as (
    -- Aggregate with conditional metrics
    select
        date_trunc('quarter', order_date) as quarter_start,
        percentile_approx(running_amount, 0.5) as running_amount_p50,
        percentile_approx(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
        sort_array(collect_set(channel)) as channel_list,
        corr(discount, amount) as corr_discount_amount,
        count(case when channel = 'store' then 1 end) as store_count,
        round(avg(discount), 2) as avg_discount
    from kept
    group by 1
)
select
    quarter_start,
    running_amount_p50,
    amount_quartiles,
    channel_list,
    corr_discount_amount,
    store_count,
    avg_discount
from summary
order by store_count desc nulls last
