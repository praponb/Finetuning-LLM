with unpacked as (
    select
        id,
        created_at,
        coupon_codes,
        items,
        attributes,
        size(filter(items, i -> i.qty >= 3)) as bulk_lines,
        transform(items, i -> i.product_id) as product_ids,
        attributes['device'] as device,
        ship_to.city as ship_city,
        forall(items, i -> i.qty <= 5) as all_small_qty
    from carts
    where to_date(created_at) >= '2025-08-09'
)
select
    case when bulk_lines >= 3 then 'HIGH' else 'LOW' end as bulk_lines_tier,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes,
    count(*) filter (where ship_city = 'Tokyo') as tokyo_rows,
    percentile_approx(bulk_lines, 0.9) as bulk_lines_p90,
    grouping(case when bulk_lines >= 3 then 'HIGH' else 'LOW' end) as grouping_level
from unpacked
group by cube (case when bulk_lines >= 3 then 'HIGH' else 'LOW' end)
order by bulk_lines_tier;
