with lines as (
    -- Unnest the array into one row per element
    select
        id,
        coupon_codes,
        k,
        v
    from carts
    lateral view outer explode(attributes) x as k, v
),
base as (
    -- Basic cleansing and defaults
    select
        *,
        initcap(trim(k)) as k_title,
        coalesce(k, 'Unknown') as k_filled
    from lines
    where array_contains(coupon_codes, 'SPRING10')
),
base_carts as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        array_join(coupon_codes, ', ') as coupon_codes_text,
        coalesce(k, 'DIRECT') as k_clean
    from base
    where array_contains(coupon_codes, 'BUNDLE')
)
-- Aggregate with conditional metrics
select
    k,
    count_if(k = 'weight_kg') as weight_kg_events,
    sort_array(collect_set(k)) as k_list,
    count(*) filter (where k = 'utm_source') as utm_source_rows,
    count_if(k = 'weight_kg') as weight_kg_events_2,
    count(distinct k) as distinct_ks,
    grouping(k) as gid
from base_carts
group by cube (k);
