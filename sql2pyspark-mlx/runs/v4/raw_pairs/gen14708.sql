with enriched as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        avg(discount) over (partition by store_id) store_id_avg_discount,
        avg(discount) over (partition by channel, store_id order by shipping_fee, id rows between 6 preceding and 1 preceding) discount_avg_6_prior,
        lead(amount, 1) over (partition by customer_id order by discount, id) next_amount,
        sum(amount) over (order by amount, id rows between unbounded preceding and current row) running_amount
    from orders
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.paid_at >= s.order_date
        )
),
enriched_orders as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        store_id_avg_discount,
        discount_avg_6_prior,
        next_amount,
        running_amount,
        sum(next_amount) over (partition by store_id order by order_date, id rows between unbounded preceding and current row) running_next_amount,
        sum(shipping_fee) over (partition by customer_id order by discount_avg_6_prior, id rows between unbounded preceding and current row) cumulative_shipping_fee,
        corr(discount, shipping_fee) over (partition by store_id) corr_discount_shipping_fee,
        sum(discount) over (partition by channel order by discount, id rows between unbounded preceding and current row) cumulative_discount
    from eligible
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status order by amount desc, id) as rank_in_group
    from enriched_orders
    qualify rank_in_group = 1
),
ranked_orders as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status, customer_id order by order_date desc, id) as latest_rank
    from ranked
    qualify latest_rank = 1
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when status in ('CANCELLED', 'COMPLETED') then 'Group A' else 'Other' end status_group,
        coalesce(channel, 'UNKNOWN') channel_filled
    from ranked_orders
    where order_date > date '2024-02-24'
)
select
    id,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    store_id_avg_discount,
    discount_avg_6_prior,
    next_amount
from valid;
