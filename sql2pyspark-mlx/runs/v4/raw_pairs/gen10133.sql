with numbered as (
    select *, row_number() over (partition by category order by price, id) as rn
    from products
)
select
    id,
    category,
    price,
    launch_date
from numbered
where rn <= 2
