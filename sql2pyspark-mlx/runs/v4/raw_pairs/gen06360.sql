select
    id,
    rating,
    supplier_name,
    city,
    coalesce(city, 'Unknown') city_filled
from suppliers
