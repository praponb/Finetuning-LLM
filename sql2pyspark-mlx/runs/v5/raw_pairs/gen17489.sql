with base as (
    select
        *,
        last_day(order_date) as order_date_month_end,
        amount + shipping_fee as amount_with_shipping
    from orders
    where order_date > date '2023-08-12'
),
with_window as (
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from base e
    inner join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.order_date_month_end >= sub.start_date
       and e.order_date_month_end < sub.end_date + interval 30 days
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by customer_id, plan order by subscription_id desc nulls first, id asc, subscription_id asc) as rn
        from with_window
    ) t
    where rn = 1
),
latest_match as (
    select
        s.*,
        x.payment_paid_at,
        x.method
    from ranked s
    left join lateral (
            select c.paid_at as payment_paid_at, c.method
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) x on true
),
with_windows as (
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        subscription_id,
        monthly_fee,
        rn,
        payment_paid_at,
        sum(amount) over (partition by channel order by rn asc, id asc, subscription_id asc rows between 3 preceding and current row) as last4_sum_amount,
        min(amount) over (partition by channel order by rn asc, id asc, subscription_id asc rows between 3 preceding and current row) as last4_min_amount,
        max(amount) over (partition by channel order by rn asc, id asc, subscription_id asc rows between 3 preceding and current row) as last4_max_amount
    from latest_match
    where status in ('SHIPPED', 'COMPLETED')
),
totals as (
    select
        case when last4_max_amount >= 1770 then 'HIGH' else 'LOW' end as last4_max_amount_tier,
        date_trunc('week', order_date) as week_start,
        sort_array(collect_set(channel)) as channel_list,
        max(payment_paid_at) as last_seen,
        sum(amount) as total_amount,
        corr(shipping_fee, amount) as corr_shipping_fee_amount,
        sum(case when channel = 'partner' then amount else 0 end) as partner_amount,
        grouping(case when last4_max_amount >= 1770 then 'HIGH' else 'LOW' end) as grouping_id
    from with_windows
    group by grouping sets ((case when last4_max_amount >= 1770 then 'HIGH' else 'LOW' end, date_trunc('week', order_date)), (case when last4_max_amount >= 1770 then 'HIGH' else 'LOW' end), ())
    having corr_shipping_fee_amount >= 0
)
select
    week_start,
    total_amount,
    corr_shipping_fee_amount,
    grouping_id
from totals
order by grouping_id asc nulls last
