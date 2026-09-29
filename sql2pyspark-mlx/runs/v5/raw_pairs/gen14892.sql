with valid as (
    -- Keep recent, valid rows
    select
        id,
        status,
        shipping_fee,
        discount / nullif(shipping_fee, 0) as discount_per_shipping_fee,
        coalesce(channel, 'NONE') as resolved_channel
    from orders
    where order_date >= date '2023-06-08'
      and customer_id is not null
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        status,
        shipping_fee,
        discount_per_shipping_fee
    from valid
    qualify row_number() over (partition by status order by shipping_fee nulls first, id) = 1
),
filtered as (
    -- Keep recent, valid rows
    select *, case when status in ('PENDING', 'RETURNED') then 'Tier 1' else 'Other' end as status_group
    from latest
    where shipping_fee between 10 and 17
),
with_latest as (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.shipping_fee,
        s.discount_per_shipping_fee,
        x.payment_amount
    from filtered s
    left join lateral (
            select c.amount as payment_amount
            from payments c
            where c.order_id = s.id
            order by c.paid_at desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lead(payment_amount) over w_grp as next_payment_amount,
        lag(shipping_fee) over w_grp as prev_shipping_fee,
        row_number() over w_grp as seq_no,
        row_number() over w_ordered as seq_no_2
    from with_latest
    window w_grp as (order by shipping_fee, id), w_ordered as (order by payment_amount, id)
)
select
    id,
    discount_per_shipping_fee,
    payment_amount,
    next_payment_amount,
    seq_no,
    seq_no_2
from enriched
order by seq_no_2 desc
