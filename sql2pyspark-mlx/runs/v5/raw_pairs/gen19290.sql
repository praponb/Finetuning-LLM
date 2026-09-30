select
    id,
    country,
    city,
    regexp_replace(supplier_name, ' ', '_') as supplier_name_slug
from suppliers
where lower(supplier_name) like '%iam%' and lower(supplier_name) like '%lob%';
