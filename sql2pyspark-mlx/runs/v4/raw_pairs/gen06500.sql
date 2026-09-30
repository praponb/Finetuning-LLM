with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        revenue,
        revenue - units as revenue_per_unit
    from store_sales
    where sale_date >= date '2023-06-13'
      and product_id in (6, 7, 9, 10)
),
joined as (
    -- Attach products attributes
    select e.*, d.product_name
    from cleaned e
    left join products d
        on e.product_id = d.id
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from joined
    qualify row_number() over (partition by product_id order by revenue_per_unit, id) = 1
)
select
    id,
    product_id,
    revenue,
    revenue_per_unit
from deduped
order by id;
