select *, lower(region) as region_lower
from stores
where sqft is null and sqft is not null
