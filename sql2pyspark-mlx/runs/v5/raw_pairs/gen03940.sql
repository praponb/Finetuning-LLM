with with_latest as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_id,
        s.status,
        s.channel,
        s.discount,
        s.shipping_fee,
        x.delivered_date,
        x.shipment_status
    from orders s
    left join lateral (
            select c.delivered_date, c.status as shipment_status
            from shipments c
            where c.order_id = s.id
              and c.shipped_date <= s.order_date
            order by c.shipped_date desc, c.id desc
            limit 1
        ) x on true
),
enriched as (
    -- Ranking and gap features
    select
        id,
        store_id,
        channel,
        discount,
        shipping_fee,
        shipment_status,
        row_number() over (partition by status, channel order by shipping_fee desc, id) as event_seq,
        avg(shipping_fee) over (partition by channel order by discount, id rows between 3 preceding and 1 preceding) as shipping_fee_avg_3_prior
    from with_latest
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by channel order by shipping_fee_avg_3_prior desc nulls last, id) as dedup_rank
    from enriched
    qualify dedup_rank = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by shipment_status, channel order by event_seq, id) as row_num
    from ranked
    qualify row_num = 1
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from deduped s
    where exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.product_id = 35
        )
),
enriched_orders as (
    -- Ranking and gap features
    select
        id,
        store_id,
        discount,
        shipping_fee,
        shipping_fee_avg_3_prior,
        dedup_rank,
        last_value(shipping_fee_avg_3_prior, true) over (partition by store_id order by dedup_rank, id rows between unbounded preceding and current row) as last_known_shipping_fee_avg_3_prior,
        avg(shipping_fee) over w_running as shipping_fee_avg_2,
        count(shipping_fee_avg_3_prior) over w_recent as shipment_status_count_shipping_fee_avg_3_prior
    from not_flagged
    window w_running as (order by shipping_fee_avg_3_prior, id rows between 2 preceding and current row), w_recent as (partition by shipment_status)
),
summary as (
    -- Aggregate with conditional metrics
    select
        store_id,
        approx_percentile(shipment_status_count_shipping_fee_avg_3_prior, array(0.25, 0.5, 0.75)) as shipment_status_count_shipping_fee_avg_3_prior_quartiles,
        count(distinct store_id) as distinct_store_ids,
        approx_percentile(shipping_fee_avg_2, array(0.25, 0.5, 0.75)) as shipping_fee_avg_2_quartiles
    from enriched_orders
    group by cube (store_id)
)
select
    store_id,
    shipment_status_count_shipping_fee_avg_3_prior_quartiles,
    distinct_store_ids,
    shipping_fee_avg_2_quartiles
from summary;
