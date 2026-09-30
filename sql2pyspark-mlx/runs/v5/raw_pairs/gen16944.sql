with base as (
    -- Keep recent, valid rows
    select *, length(channel) as channel_length
    from orders
    where order_date >= date '2023-04-05'
),
with_window as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from base e
    left join promotions pr
        on e.channel <=> pr.channel
       and e.order_date >= pr.starts_on
       and e.order_date <= date_add(pr.ends_on, 10)
),
latest_match as (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.shipment_cost,
        latest.delivered_date
    from with_window s
    join lateral (
            select c.cost as shipment_cost, c.delivered_date
            from shipments c
            where c.order_id = s.id
              and c.delivered_date <= s.order_date
            order by c.delivered_date desc, c.id desc
            limit 1
        ) latest on true
),
with_latest as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_id,
        s.channel,
        s.promo_id,
        s.promo_code,
        s.discount_pct,
        s.shipment_cost,
        x.order_item_unit_price,
        x.order_item_quantity
    from latest_match s
    join lateral (
            select c.unit_price as order_item_unit_price, c.quantity as order_item_quantity
            from order_items c
            where c.order_id = s.id
            order by c.unit_price desc, c.id desc
            limit 1
        ) x on true
)
-- Aggregate with conditional metrics
select
    promo_id,
    size(collect_set(promo_code)) as num_promo_codes,
    sort_array(collect_set(promo_code)) as promo_code_list,
    percentile_approx(order_item_quantity, array(0.25, 0.5, 0.75)) as order_item_quantity_quartiles
from with_latest
group by promo_id
having size(collect_set(promo_code)) >= 1
order by promo_id nulls first;
