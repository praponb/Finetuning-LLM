select id, store_name, coalesce(sqft, 0) as sqft_filled
from stores
where city in ('Chiang Mai', 'London', 'Tokyo') and sqft != 15456;
