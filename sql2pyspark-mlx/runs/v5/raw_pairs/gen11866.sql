select *
from (
    select
        supplier_id,
        brand,
        category,
        price
    from products
) src
pivot (
    max(price)
    for category in ('Electronics' as electronics, 'Home' as home)
)
