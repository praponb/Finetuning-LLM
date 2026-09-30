select
    id,
    country,
    age,
    row_number() over (partition by country order by age, id) as row_num
from customers
order by id desc;
