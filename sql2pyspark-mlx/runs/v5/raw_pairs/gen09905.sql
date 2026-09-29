with ranked as (
    select
        id,
        category,
        price,
        supplier_id,
        row_number() over (partition by category order by price desc, id) as pos
    from products
)
select *
from ranked
where pos <= 3
order by supplier_id desc, pos desc;
