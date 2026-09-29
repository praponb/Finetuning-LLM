select *, lower(supplier_name) as supplier_name_lower, case when country in ('TH', 'UK') then 'Priority' else 'Other' end as country_group
from suppliers
where city <> 'Bangkok'
order by id;
