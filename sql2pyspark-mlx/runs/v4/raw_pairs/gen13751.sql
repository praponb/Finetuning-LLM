select *, upper(country) as country_upper
from suppliers
where supplier_name like 'Glo%' and country = 'US'
order by country desc;
