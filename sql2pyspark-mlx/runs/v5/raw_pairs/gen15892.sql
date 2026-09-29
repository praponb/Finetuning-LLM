select
    id,
    city,
    rating,
    trim(city) as clean_city
from suppliers
where rating <= 2.9 and supplier_name is not null and upper(supplier_name) like '%IAM%';
