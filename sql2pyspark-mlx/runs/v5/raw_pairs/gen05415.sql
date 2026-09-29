select *
from stores
where city not in ('Berlin', 'London') and sqft >= 14552 and city not in ('London', 'Chiang Mai', 'Berlin');
