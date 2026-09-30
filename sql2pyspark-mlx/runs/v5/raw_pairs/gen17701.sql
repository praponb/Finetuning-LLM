select store_name, opened_date
from stores
where city not in ('New York', 'Tokyo') and upper(trim(store_name)) = 'SIAM FLAGSHIP'
order by store_name
