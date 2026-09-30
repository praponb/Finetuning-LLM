select
    id,
    segment,
    credit_limit,
    age
from (
    select *, row_number() over (partition by segment order by credit_limit desc, id) as pos
    from customers
) as t
where pos = 1
order by credit_limit desc;
