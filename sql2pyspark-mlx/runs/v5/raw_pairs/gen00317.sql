select id, carrier, cost
from (
    select *, row_number() over (partition by carrier order by cost, id) as rnk
    from shipments
    where status = 'RETURNED'
) as t
where rnk = 1
order by id;
