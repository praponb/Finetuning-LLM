select *
from (
    select
        id,
        supplier_id,
        cost,
        category,
        row_number() over (partition by supplier_id order by cost desc, id) as rn
    from products
) as ranked
where rn = 1
order by category;
