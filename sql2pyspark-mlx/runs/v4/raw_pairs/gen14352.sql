with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when city = 'Berlin' then 1 else 0 end as is_berlin,
        coalesce(city, 'N/A') as city_clean
    from suppliers
    where supplier_name is not null
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        supplier_name,
        country,
        city,
        city_clean,
        upper(supplier_name) as supplier_name_upper
    from base
    where supplier_name like 'E%'
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.product_category,
        latest.brand
    from cleaned s
    left join lateral (
            select c.category as product_category, c.brand
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) latest on true
),
base_suppliers as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws(',', city, brand) as city_brand_key,
        case when product_category in ('Electronics', 'Sports') then 'Priority' else 'Other' end as product_category_group
    from with_latest
    where city is not null
)
select
    id,
    supplier_name,
    product_category,
    brand,
    city_brand_key,
    product_category_group,
    case when product_category rlike '^(Sports|Grocery)$' then 'OTHER_ZONE' else 'GROUP_A' end as product_category_zone
from base_suppliers
order by brand desc, product_category;
