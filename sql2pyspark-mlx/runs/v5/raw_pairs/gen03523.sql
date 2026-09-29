with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        upper(status) as status_upper,
        amount / nullif(shipping_fee, 0) as amount_per_shipping_fee
    from orders
    where order_date >= date '2023-07-17'
      and channel is not null
),
scored as (
    -- Ranking and gap features
    select
        *,
        max(amount_per_shipping_fee) over (partition by channel, store_id) as channel_max_amount_per_shipping_fee,
        sum(amount_per_shipping_fee) over (partition by channel order by discount, id rows between unbounded preceding and current row) as running_amount_per_shipping_fee,
        rank() over w_part as discount_rank
    from filtered
    window w_part as (partition by customer_id, store_id order by discount desc nulls first)
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        store_id,
        order_date,
        discount,
        status_upper,
        amount_per_shipping_fee,
        running_amount_per_shipping_fee,
        rank() over (partition by store_id order by amount_per_shipping_fee desc nulls first) as amount_per_shipping_fee_rank,
        ntile(10) over (partition by customer_id, channel order by running_amount_per_shipping_fee, id) as running_amount_per_shipping_fee_bucket,
        sum(channel_max_amount_per_shipping_fee) over (partition by customer_id, status order by discount, id rows between unbounded preceding and current row) as running_channel_max_amount_per_shipping_fee
    from scored
),
enriched as (
    -- Attach stores attributes
    select
        src.id,
        src.store_id,
        src.order_date,
        src.discount,
        src.status_upper,
        src.amount_per_shipping_fee,
        src.amount_per_shipping_fee_rank,
        d.region
    from metrics src
    left join stores d
        on src.store_id = d.id
    where d.region in ('West', 'South', 'East')
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            store_id,
            region,
            discount
        from enriched
    ) src
    pivot (
        avg(discount) as total,
        count(1) as n
        for region in ('Central', 'South')
    )
)
select
    s.store_id,
    s.Central_total,
    s.Central_n,
    s.South_total,
    (select avg(s2.units) from store_sales s2 where s2.store_id = s.store_id) as avg_store_sale_units
from by_type s
