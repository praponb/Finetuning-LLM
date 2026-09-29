select
    id,
    category,
    price,
    size(tags) as tags_count
from products
where not (supplier_id in (2, 28) and brand != 'Acme')
