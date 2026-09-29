with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        trim(status) as clean_status
    from orders
    where channel is not null
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from cleaned s
    where not exists (
            select 1
            from payments c
            where c.order_id = s.id
              and c.paid_at >= s.order_date
        )
),
enriched as (
    -- Ranking and gap features
    select
        *,
        row_number() over w_hist as event_seq,
        sum(amount) over (order by shipping_fee, id rows between unbounded preceding and current row) as running_amount,
        corr(discount, amount) over (partition by channel, status) as corr_discount_amount,
        sum(shipping_fee) over (partition by store_id order by amount, id rows between unbounded preceding and current row) as running_shipping_fee
    from kept
    window w_hist as (partition by store_id order by amount desc, id)
),
kept_orders as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.shipped_date <= s.order_date
        )
),
with_latest as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select s.*, x.shipment_delivered_date
    from kept_orders s
    left join lateral (
            select c.delivered_date as shipment_delivered_date
            from shipments c
            where c.order_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) x on true
)
-- Multi-level aggregation
select
    status,
    count(*) filter (where channel = 'mobile') as mobile_rows,
    count(distinct store_id) as distinct_store_ids,
    sum(case when status = 'COMPLETED' then event_seq else 0 end) as completed_event_seq
from with_latest
group by status
having mobile_rows < 1
