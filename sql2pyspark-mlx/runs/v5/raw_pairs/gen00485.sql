with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        order_date,
        status,
        discount,
        shipping_fee,
        trim(channel) clean_channel,
        round(shipping_fee / amount, 2) amount_with_shipping
    from orders
    where channel is not null
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by store_id, status order by order_date desc, id) as rank_in_group
    from valid
    qualify rank_in_group = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        shipping_fee,
        amount_with_shipping
    from (
        select *, row_number() over (partition by store_id order by rank_in_group, id) as rank_in_group_2
        from latest
    ) t
    where rank_in_group_2 = 1
),
with_latest as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.shipping_fee,
        s.amount_with_shipping,
        x.cost
    from ranked s
    join lateral (
            select c.cost
            from shipments c
            where c.order_id = s.id
            order by c.shipped_date desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and ch.method in ('bank_transfer', 'paypal', 'card')
        )
)
-- Grouped metrics
select
    case when cost >= 87 then 'LARGE' else 'SMALL' end as cost_tier,
    case when amount_with_shipping >= 61 then 'HIGH' else 'LOW' end as amount_with_shipping_tier,
    approx_percentile(shipping_fee, array(0.25, 0.5, 0.75)) as shipping_fee_quartiles,
    stddev_samp(shipping_fee) as shipping_fee_stddev,
    round(avg(shipping_fee), 2) as avg_shipping_fee,
    percentile_approx(amount_with_shipping, array(0.25, 0.5, 0.75)) as amount_with_shipping_quartiles,
    percentile_approx(cost, 0.5) as cost_p50,
    count(*) as num_rows
from kept
group by case when cost >= 87 then 'LARGE' else 'SMALL' end, case when amount_with_shipping >= 61 then 'HIGH' else 'LOW' end
having num_rows < 1000
order by amount_with_shipping_tier desc
