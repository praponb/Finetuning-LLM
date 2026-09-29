select id, category, price
from (
    select
        id,
        category,
        price,
        row_number() over (partition by category order by price desc, id) as pos
    from products
    where supplier_id is not null
) as r
where pos <= 2;
