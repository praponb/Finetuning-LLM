with top_products as (
    select
        id,
        supplier_id,
        price,
        row_number() over (partition by supplier_id order by price, id) as row_num
    from products
    where array_contains(tags, 'eco')
)
select *
from top_products
where row_num <= 1
order by supplier_id;
