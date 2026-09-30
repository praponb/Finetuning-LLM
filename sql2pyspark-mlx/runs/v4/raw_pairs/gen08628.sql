select supplier_name, country
from suppliers
where country in ('DE', 'UK', 'TH')
union all
select supplier_name, country
from suppliers
where city in ('New York', 'Singapore');
