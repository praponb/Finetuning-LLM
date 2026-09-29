select
    id,
    country,
    upper(supplier_name) as supplier_name_upper,
    round(rating * 1.15, 2) as adjusted_rating
from suppliers
where country = 'DE'
order by id desc
