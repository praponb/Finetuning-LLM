with flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        created_at,
        items,
        try_cast(attributes['weight_kg'] as double) as weight_kg,
        coalesce(ship_to.city, 'UNKNOWN') as city_or_unknown,
        size(items) as num_items
    from carts
)
-- Multi-level aggregation
select
    date_trunc('hour', created_at) as hour_window,
    min(created_at) as started_at,
    min(created_at) as started_at_2
from flattened_attrs
group by hour_window
order by hour_window nulls first;
