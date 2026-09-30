select *, case when country in ('DE', 'JP') then 'Priority' else 'Other' end country_group, substring(supplier_name, 1, 3) supplier_name_prefix
from suppliers
order by rating desc, id
limit 1
