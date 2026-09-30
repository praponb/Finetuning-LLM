with latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status order by paid_at desc nulls first, id) as latest_rank
    from payments
    qualify latest_rank = 1
),
joined as (
    -- Attach orders attributes
    select
        s.id,
        s.order_id,
        s.amount,
        s.paid_at,
        s.latest_rank,
        o.shipping_fee
    from latest s
    left join orders o
        on s.order_id = o.id
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        min(amount) over (partition by order_id order by shipping_fee, id rows between unbounded preceding and current row) as running_min_amount,
        avg(amount) over (partition by order_id order by shipping_fee, id rows between unbounded preceding and current row) as running_avg_amount,
        max(amount) over (partition by order_id order by shipping_fee, id rows between unbounded preceding and current row) as running_max_amount,
        avg(amount) over (partition by order_id order by paid_at, id rows between 3 preceding and 1 preceding) as amount_avg_3_prior
    from joined
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        order_id,
        amount,
        paid_at,
        shipping_fee,
        running_min_amount,
        running_max_amount,
        amount_avg_3_prior
    from (
        select *, row_number() over (partition by order_id order by amount_avg_3_prior nulls first, id) as dedup_rank
        from metrics
    ) x
    where dedup_rank = 1
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by order_id order by amount_avg_3_prior desc, id) as row_num
    from deduped
    qualify row_num = 1
)
select
    id,
    order_id,
    paid_at,
    shipping_fee,
    running_max_amount
from ranked
order by order_id desc
