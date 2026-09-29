with with_dims as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.store_id,
        e.status,
        e.discount,
        e.shipping_fee,
        d.city
    from orders e
    left join customers d
        on e.customer_id = d.id
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_dims s
    where not exists (
            select 1
            from order_items x
            where x.order_id = s.id
              and x.product_id = 17
        )
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        discount,
        lead(discount) over (partition by store_id order by shipping_fee, id) as next_discount,
        lag(discount) over (partition by store_id order by shipping_fee, id) as prev_discount,
        lead(shipping_fee) over (partition by store_id order by shipping_fee, id) as next_shipping_fee,
        row_number() over (partition by store_id order by shipping_fee, id) as event_seq,
        avg(discount) over (partition by customer_id order by discount, id rows between 3 preceding and current row) as discount_avg_3
    from eligible
),
scored as (
    -- Running and moving aggregates
    select
        *,
        avg(next_discount) over (partition by store_id order by next_shipping_fee, id rows between 6 preceding and 1 preceding) as next_discount_avg_6_prior,
        row_number() over (partition by store_id order by prev_discount desc, id) as seq_no
    from enriched
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from scored s
    where exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.order_id in (9, 10, 11, 14)
        )
),
kept_orders as (
    -- Semi-join via a correlated subquery
    select s.*
    from kept s
    where exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and ch.order_id is not null
        )
),
with_latest as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select s.*, top1.payment_amount
    from kept_orders s
    join lateral (
            select c.amount as payment_amount
            from payments c
            where c.order_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
)
-- Aggregate with conditional metrics
select
    case when payment_amount >= 1640 then 'LARGE' else 'SMALL' end as payment_amount_tier,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    percentile_approx(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles
from with_latest
group by case when payment_amount >= 1640 then 'LARGE' else 'SMALL' end
