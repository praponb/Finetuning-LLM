select *, concat(supplier_name, ', ', country) as supplier_name_country
from suppliers
where rating is null and rating <= 2.5;
