with valid as (
    -- Basic cleansing and defaults
    select *, round(amount, 2) as amount_rounded
    from orders
    where order_date >= to_date('2023-08-18')
      and channel is not distinct from 'store'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        sum(amount_rounded) over (partition by customer_id order by discount, id rows between unbounded preceding and current row) as cumulative_amount_rounded,
        lead(shipping_fee, 1) over (partition by store_id order by discount, id) as next_shipping_fee
    from valid
),
with_last as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.order_date,
        s.status,
        s.amount,
        s.shipping_fee,
        s.amount_rounded,
        s.cumulative_amount_rounded,
        s.next_shipping_fee,
        latest.shipment_carrier,
        latest.shipment_status
    from with_windows s
    join lateral (
            select c.carrier as shipment_carrier, c.status as shipment_status
            from shipments c
            where c.order_id = s.id
              and c.delivered_date <= s.order_date
            order by c.delivered_date desc, c.id desc
            limit 1
        ) latest on true
),
with_windows_orders as (
    -- Running and moving aggregates
    select
        id,
        shipping_fee,
        amount_rounded,
        cumulative_amount_rounded,
        next_shipping_fee,
        shipment_status,
        lead(cumulative_amount_rounded, 2) over (order by cumulative_amount_rounded, id) as next_cumulative_amount_rounded,
        stddev_samp(amount) over (partition by shipment_carrier, shipment_status) as shipment_carrier_stddev_samp_amount,
        percent_rank() over w_hist as amount_rounded_percent_rank,
        avg(amount_rounded) over (partition by shipment_status order by amount, id rows between 3 preceding and 1 preceding) as amount_rounded_avg_3_prior,
        sum(next_shipping_fee) over w_grp as cumulative_next_shipping_fee
    from with_last
    where amount <= 740
    window w_hist as (order by amount_rounded), w_grp as (order by amount, id rows between unbounded preceding and current row)
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows_orders s
    where not exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.paid_at < '2023-03-16'
        )
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from not_flagged
    qualify row_number() over (partition by shipment_status order by amount_rounded_percent_rank desc nulls first, id) = 1
),
stats as (
    -- Aggregate with conditional metrics
    select
        shipment_status,
        case when shipping_fee >= 44 then 'PREMIUM' else 'STANDARD' end as shipping_fee_tier,
        stddev_samp(shipping_fee) as shipping_fee_stddev,
        sort_array(collect_set(shipment_status)) as shipment_status_list,
        size(collect_set(shipment_status)) as num_shipment_status,
        round(sum(amount_rounded) / nullif(count(case when shipment_status = 'IN_TRANSIT' then 1 end), 0), 2) as amount_rounded_per_in_transit,
        sum(case when shipment_status = 'DELIVERED' then next_cumulative_amount_rounded else 0 end) as delivered_next_cumulative_amount_rounded
    from latest
    group by 1, 2
    having size(collect_set(shipment_status)) > 1
)
select
    shipment_status,
    shipping_fee_tier,
    shipping_fee_stddev,
    shipment_status_list,
    delivered_next_cumulative_amount_rounded
from stats
order by delivered_next_cumulative_amount_rounded desc;
