select
    id,
    category,
    brand,
    price,
    tags
from products
qualify row_number() over (partition by brand order by cost asc nulls last, id asc) = 1
