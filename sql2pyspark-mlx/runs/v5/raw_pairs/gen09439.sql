with parsed as (
    select
        id,
        created_at,
        ship_to,
        attributes,
        size(filter(items, item -> item.qty >= 3)) as bulk_lines,
        ship_to.city as ship_city,
        array_max(transform(items, item -> item.price)) as max_item_price,
        exists(coupon_codes, c -> c like 'W%') as has_welcome_code
    from carts
)
select
    case when max_item_price >= 192 then 'PREMIUM' else 'STANDARD' end as max_item_price_tier,
    ship_city,
    min(created_at) as first_created_at,
    round(avg(max_item_price), 2) as avg_max_item_price,
    sum(case when ship_city = 'Tokyo' then bulk_lines else 0 end) as tokyo_bulk_lines,
    count(*) filter (where ship_city = 'Singapore') as singapore_rows
from parsed
group by case when max_item_price >= 192 then 'PREMIUM' else 'STANDARD' end, ship_city
order by avg_max_item_price nulls first;
