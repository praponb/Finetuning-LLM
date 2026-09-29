with with_dims as (
    select
        e.*,
        o.shipping_fee,
        o.order_date
    from shipments e
    left join orders o
        on e.order_id = o.id
    where o.amount is not null
),
deduped as (
    select
        id,
        order_id,
        carrier,
        order_date
    from (
        select *, row_number() over (partition by carrier, order_id order by shipped_date nulls last, id) as rn
        from with_dims
    ) ranked
    where rn = 1
),
deduped_shipments as (
    select id, carrier
    from deduped
    qualify rank() over (partition by order_id order by order_date nulls last) = 1
),
base as (
    select
        *,
        initcap(trim(carrier)) carrier_title,
        length(carrier) carrier_length
    from deduped_shipments
    where carrier is null
),
valid as (
    select *, substring(carrier, 1, 3) carrier_prefix
    from base
    where carrier is not null
      and carrier_length between 28 and 41
),
ranked as (
    select *
    from valid
    qualify row_number() over (partition by carrier order by carrier_length desc, id) = 1
),
enriched as (
    select
        id,
        carrier,
        carrier_prefix,
        avg(carrier_length) over (partition by carrier order by carrier_length, id rows between 6 preceding and current row) carrier_length_avg_6,
        cume_dist() over (partition by carrier order by carrier_length desc nulls last) carrier_length_cume_dist
    from ranked
)
select
    case when carrier_length_cume_dist >= 0.77 then 'LARGE' else 'SMALL' end as carrier_length_cume_dist_tier,
    carrier,
    avg(carrier_length_avg_6) filter (where carrier = 'Kerry') as kerry_avg_carrier_length_avg_6,
    round(sum(carrier_length_cume_dist) / nullif(count(case when carrier = 'UPS' then 1 end), 0), 2) as carrier_length_cume_dist_per_ups,
    round(sum(carrier_length_avg_6) / nullif(count(case when carrier = 'Kerry' then 1 end), 0), 2) as carrier_length_avg_6_per_kerry,
    round(sum(carrier_length_avg_6) / nullif(count(case when carrier = 'UPS' then 1 end), 0), 2) as carrier_length_avg_6_per_ups,
    grouping(case when carrier_length_cume_dist >= 0.77 then 'LARGE' else 'SMALL' end) as grouping_id
from enriched
group by grouping sets ((case when carrier_length_cume_dist >= 0.77 then 'LARGE' else 'SMALL' end, carrier), (case when carrier_length_cume_dist >= 0.77 then 'LARGE' else 'SMALL' end))
order by kerry_avg_carrier_length_avg_6 desc
