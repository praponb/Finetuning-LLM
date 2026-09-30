with latest as (
    select *
    from (
        select *, row_number() over (partition by brand order by price desc, id) as rank_in_group
        from products
    ) x
    where rank_in_group = 1
)
select
    id,
    product_name,
    price,
    cost,
    tags,
    supplier_id,
    rank_in_group,
    case when product_name rlike '^(Desk Lamp|USB-C Hub)$' then 'GROUP_A' when product_name rlike '^(Laptop Pro 14|wireless mouse)$' then 'EXTENDED' else 'OTHER_ZONE' end as product_name_zone
from latest
order by supplier_id desc nulls first
