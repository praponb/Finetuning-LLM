select
    id,
    city,
    credit_limit,
    name
from (
    select *, row_number() over (partition by city order by credit_limit desc, id) as pos
    from customers
    where credit_limit is null
) as ranked
where pos <= 3
order by city;
