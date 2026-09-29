with valid as (
    select *, initcap(trim(status)) as status_title
    from orders
    where store_id is not null
      and discount between 0.15 and 0.2
),
with_last as (
    select
        s.*,
        latest.delivered_date,
        latest.shipment_carrier
    from valid s
    left join lateral (
            select c.delivered_date, c.carrier as shipment_carrier
            from shipments c
            where c.order_id = s.id
              and c.shipped_date <= s.order_date
            order by c.shipped_date desc, c.id desc
            limit 1
        ) latest on true
),
kept as (
    select s.*
    from with_last s
    where not exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and ch.paid_at < '2024-08-15'
        )
),
scored as (
    select
        id,
        customer_id,
        store_id,
        status,
        amount,
        discount,
        status_title,
        shipment_carrier,
        sum(discount) over w_recent as last4_sum_discount,
        avg(discount) over w_recent as last4_avg_discount,
        count(discount) over w_recent as last4_count_discount
    from kept
    window w_recent as (partition by status order by delivered_date asc, id asc rows between 3 preceding and current row)
),
not_flagged as (
    select s.*
    from scored s
    where exists (
            select 1
            from payments x
            where x.order_id = s.id
              and x.status not in ('SUCCESS', 'FAILED')
        )
)
select
    case when last4_sum_discount >= 1.77 then 'HIGH' else 'LOW' end as last4_sum_discount_tier,
    case when last4_count_discount >= 14 then 'LARGE' else 'SMALL' end as last4_count_discount_tier,
    count_if(shipment_carrier = 'DHL') as dhl_events,
    percentile_approx(discount, 0.9) as discount_p90,
    round(avg(last4_avg_discount), 2) as avg_last4_avg_discount,
    count(*) as num_rows,
    count(*) filter (where status = 'RETURNED') as returned_rows,
    min(discount) as min_discount,
    grouping_id() as grouping_level
from not_flagged
group by cube (case when last4_sum_discount >= 1.77 then 'HIGH' else 'LOW' end, case when last4_count_discount >= 14 then 'LARGE' else 'SMALL' end)
having num_rows > 1;
