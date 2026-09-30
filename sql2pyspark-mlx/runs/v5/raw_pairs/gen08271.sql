select id, supplier_id, brand
from products
where launch_date <= '2023-04-01' and (brand <> 'Initech' or supplier_id in (8, 28, 31))
order by brand asc
