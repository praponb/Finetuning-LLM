select id, country, coalesce(city, 'unassigned') as city_filled
from suppliers
where rating >= 4.2
