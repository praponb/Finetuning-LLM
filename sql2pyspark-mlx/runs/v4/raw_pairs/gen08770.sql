select id, rating
from suppliers
where rating >= (select avg(rating) from suppliers where country not in ('TH', 'US', 'SG'))
