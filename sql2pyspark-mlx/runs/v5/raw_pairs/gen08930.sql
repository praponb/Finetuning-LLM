with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        store_id,
        status,
        amount,
        shipping_fee,
        dayofweek(order_date) as order_date_dayofweek,
        shipping_fee - discount as shipping_fee_minus_discount
    from orders
    where order_date > to_date('2023-05-12')
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(shipping_fee) over (order by order_date_dayofweek, id rows between unbounded preceding and current row) as cumulative_shipping_fee,
        sum(order_date_dayofweek) over (partition by store_id order by amount, id rows between unbounded preceding and current row) as running_order_date_dayofweek,
        lead(amount, 1) over (order by order_date_dayofweek, id) as next_amount
    from cleaned
),
latest_match as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select s.*, x.payment_status
    from metrics s
    join lateral (
            select c.status as payment_status
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        last_value(shipping_fee_minus_discount, true) over (partition by store_id order by shipping_fee_minus_discount, id rows between unbounded preceding and current row) as last_known_shipping_fee_minus_discount,
        lead(order_date_dayofweek, 1, 0) over w_recent as next_order_date_dayofweek,
        lead(running_order_date_dayofweek, 1, 0) over (partition by status, payment_status order by cumulative_shipping_fee, id) as next_running_order_date_dayofweek,
        lead(shipping_fee_minus_discount, 1) over (partition by payment_status order by shipping_fee, id) as next_shipping_fee_minus_discount
    from latest_match
    where shipping_fee_minus_discount >= 68
    window w_recent as (partition by store_id order by running_order_date_dayofweek, id)
)
-- Grouped metrics
select
    status,
    case when cumulative_shipping_fee >= 152 then 'HIGH' else 'LOW' end as cumulative_shipping_fee_tier,
    case when next_running_order_date_dayofweek >= 307 then 'HIGH' else 'LOW' end as next_running_order_date_dayofweek_tier,
    count(distinct status) as distinct_status,
    sum(cumulative_shipping_fee) filter (where payment_status = 'FAILED') as failed_total_cumulative_shipping_fee,
    sum(case when status = 'PENDING' then running_order_date_dayofweek else 0 end) as pending_running_order_date_dayofweek
from with_windows
group by grouping sets ((status, case when cumulative_shipping_fee >= 152 then 'HIGH' else 'LOW' end, case when next_running_order_date_dayofweek >= 307 then 'HIGH' else 'LOW' end), (status, case when cumulative_shipping_fee >= 152 then 'HIGH' else 'LOW' end), (status))
order by next_running_order_date_dayofweek_tier desc nulls last
