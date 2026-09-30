select
    id,
    city,
    age,
    row_number() over (partition by city order by age desc, id) as age_row
from customers
where name like '%Che%';
