with parsed as (
    -- Higher-order array functions
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        ship_to,
        array_contains(map_keys(attributes), 'utm_source') as has_utm_source,
        coalesce(attributes['gift'], 'direct') as gift_or_default,
        coalesce(ship_to.city, 'UNKNOWN') as city_or_unknown,
        size(attributes) as num_attributes,
        attributes['gift'] as gift
    from carts
)
-- Grouped metrics
select
    date_trunc('day', created_at) as activity_day,
    round(avg(num_attributes), 2) as avg_num_attributes,
    count(distinct city_or_unknown) as distinct_city_or_unknowns,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes
from parsed
group by date_trunc('day', created_at);
