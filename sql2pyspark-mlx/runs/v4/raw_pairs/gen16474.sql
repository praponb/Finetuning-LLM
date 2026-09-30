with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from products s
    where not exists (
            select 1
            from order_items ch
            where ch.product_id = s.id
              and ch.product_id is not null
        )
),
not_flagged_products as (
    -- Anti-join via a correlated subquery
    select s.*
    from not_flagged s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.sale_date >= s.launch_date
        )
),
enriched as (
    -- Attach suppliers attributes
    select
        src.*,
        b.city,
        b.supplier_name
    from not_flagged_products src
    inner join suppliers b
        on src.supplier_id = b.id
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        city,
        supplier_name,
        sort_array(tags) as sorted_tags
    from enriched
    where launch_date > date '2023-10-23'
      and supplier_id is not null
      and product_name is not null
)
select
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    city,
    supplier_name,
    case when supplier_name rlike '^(Pacific Wholesale|Global Supply)$' then 'CORE' else 'PRIORITY' end as supplier_name_zone
from base
order by city desc
