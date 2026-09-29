select
    id,
    tags,
    upper(trim(product_name)) as product_name_normalized,
    substring(brand, 1, 3) as brand_prefix
from products
where price < 880
order by product_name_normalized, id
limit 20;
