select credit_limit, country
from customers
where (country <> 'DE' and name like '%Kanya%') or signup_date between '2024-05-30' and '2024-08-24'
order by country asc;
