with ranked as (
    -- Keep one row per key (latest first)
    select
        id,
        order_id,
        amount
    from payments
    qualify row_number() over (partition by status order by paid_at desc, id) = 1
),
ranked_payments as (
    -- Keep one row per key (latest first)
    select *
    from ranked
    qualify row_number() over (partition by order_id order by amount desc, id) = 1
),
ranked_payments_2 as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by order_id order by amount desc nulls first, id) as rn
    from ranked_payments
    qualify rn = 1
),
enriched as (
    -- Attach orders attributes
    select a.*, o.order_date
    from ranked_payments_2 a
    left join orders o
        on a.order_id = o.id
),
filtered as (
    -- Keep recent, valid rows
    select *, date_format(order_date, 'MMM yyyy') as order_date_label
    from enriched
    where order_id is not null
),
with_dims as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.order_date,
        src.order_date_label,
        b.channel,
        b.amount as order_amount
    from filtered src
    left join orders b
        on src.order_id = b.id
)
-- Multi-level aggregation
select
    case when order_amount >= 1450 then 'LARGE' else 'SMALL' end as order_amount_tier,
    sort_array(collect_set(channel)) as channel_list,
    round(sum(order_amount) / nullif(count(case when channel = 'store' then 1 end), 0), 2) as order_amount_per_store,
    percentile_approx(order_amount, 0.5) as order_amount_p50
from with_dims
group by case when order_amount >= 1450 then 'LARGE' else 'SMALL' end
having order_amount_per_store > 0
