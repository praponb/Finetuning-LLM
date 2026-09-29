select
    src.id,
    src.store_id,
    src.product_id,
    src.units,
    src.revenue,
    b.price,
    b.category
from store_sales src
join products b
    on src.product_id = b.id
