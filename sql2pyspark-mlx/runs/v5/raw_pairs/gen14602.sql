select
    id,
    city,
    round(rating * 0.85, 2) as adjusted_rating,
    coalesce(supplier_name, 'Unknown') as supplier_name_filled
from suppliers
