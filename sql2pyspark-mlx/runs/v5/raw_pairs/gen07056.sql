with base as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        quantity,
        round(quantity, 2) quantity_rounded
    from inventory
    where store_id in (2, 3, 5, 6)
),
with_dims as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        s.product_id,
        s.quantity_rounded,
        d.city
    from base s
    left join stores d
        on s.store_id = d.id
    where d.store_name not like '%Riversid%'
),
enriched as (
    -- Attach products attributes
    select
        e.id,
        e.product_id,
        e.city,
        b.launch_date,
        b.brand
    from with_dims e
    join products b
        on e.product_id = b.id
),
matched_keys as (
    -- Set difference on keys
    select product_id, id
    from enriched
    except
    select product_id, id
    from inventory
    where store_id in (1, 3, 4, 5)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join matched_keys k on s.product_id = k.product_id and s.id = k.id
)
-- Grouped metrics
select
    city,
    brand,
    count(distinct product_id) as distinct_product_ids,
    sort_array(collect_set(city)) as city_list
from verified
group by city, brand
order by city nulls last
