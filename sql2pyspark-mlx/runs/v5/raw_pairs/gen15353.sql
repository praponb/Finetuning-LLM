select
    id,
    country,
    supplier_name,
    concat(country, ' ', supplier_name) as country_supplier_name,
    lower(supplier_name) as supplier_name_lower
from suppliers
where supplier_name is null
order by country, 5 desc;
