select
    id,
    segment,
    signup_date,
    ntile(10) over (partition by segment order by signup_date, id) bucket
from customers
where is_active = true;
