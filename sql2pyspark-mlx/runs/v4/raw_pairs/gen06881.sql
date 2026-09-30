select category, brand
from products
intersect all
select category, brand
from products
where month(launch_date) in (6, 8, 12)
