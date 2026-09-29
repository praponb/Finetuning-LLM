with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from orders s
    where not exists (
            select 1
            from order_items c
            where c.order_id = s.id
              and c.unit_price <= 380
        )
),
enriched as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        avg(amount) over (partition by status order by discount, id rows between 2 preceding and current row) as last3_avg_amount,
        max(amount) over (partition by status order by discount, id rows between 2 preceding and current row) as last3_max_amount,
        min(amount) over (partition by status order by discount, id rows between 2 preceding and current row) as last3_min_amount,
        sum(amount) over (partition by status order by discount, id rows between 2 preceding and current row) as last3_sum_amount,
        row_number() over (partition by channel order by shipping_fee desc, id) as seq_no
    from not_flagged
),
with_window as (
    -- Non-equi join on a validity window
    select
        a.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from enriched a
    left join subscriptions sub
        on a.customer_id = sub.customer_id
       and a.order_date >= sub.start_date
       and a.order_date < sub.end_date + interval '14' day
),
joined as (
    -- Attach customers attributes
    select
        e.*,
        d.signup_date,
        d.country,
        d.credit_limit
    from with_window e
    left join customers d
        on e.customer_id <=> d.id
)
-- Grouped metrics
select
    date_trunc('week', order_date) as week_start,
    customer_id,
    size(collect_set(plan)) as num_plans,
    sum(case when plan = 'basic' then last3_max_amount else 0 end) as basic_last3_max_amount,
    round(avg(last3_avg_amount), 2) as avg_last3_avg_amount,
    count(distinct status) as distinct_status,
    corr(discount, amount) as corr_discount_amount,
    percentile_approx(seq_no, 0.95) as seq_no_p95
from joined
group by date_trunc('week', order_date), customer_id
having percentile_approx(seq_no, 0.95) >= 0
order by corr_discount_amount desc;
