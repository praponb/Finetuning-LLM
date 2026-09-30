with base as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        add_months(order_date, 6) as order_date_plus_months
    from orders
    where order_date > to_date('2023-09-17')
),
kept_keys as (
    -- Set difference on keys
    select store_id, customer_id
    from base
    except all
    select store_id, customer_id
    from orders
    where amount < 1220
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    inner join kept_keys k on s.store_id = k.store_id and s.customer_id = k.customer_id
),
scored as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        order_date_plus_months,
        corr(discount, amount) over w_ordered as corr_discount_amount,
        count(discount) over w_ordered_2 as channel_count_discount,
        count(amount) over w_ordered as customer_id_count_amount
    from reconciled
    window w_ordered as (partition by customer_id), w_ordered_2 as (partition by channel)
)
-- Aggregate with conditional metrics
select
    store_id,
    size(collect_set(channel)) as num_channels,
    round(sum(corr_discount_amount) / nullif(count(case when channel = 'mobile' then 1 end), 0), 2) as corr_discount_amount_per_mobile,
    avg(customer_id_count_amount) as avg_customer_id_count_amount
from scored
group by 1
having size(collect_set(channel)) > 1
order by num_channels desc nulls last;
