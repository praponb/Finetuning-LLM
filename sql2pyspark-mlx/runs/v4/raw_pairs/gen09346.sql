select
    id,
    product_name,
    category,
    size(tags) as tags_count
from products
where price < 680 and (size(tags) > 1 or supplier_id is null)
order by tags_count asc
