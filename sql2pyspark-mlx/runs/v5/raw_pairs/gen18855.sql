with Numbered as (
    select *, row_number() over (partition by status order by paid_at desc, id) as rnk
    from payments
)
select id, status, paid_at
from Numbered
where rnk = 1
order by status asc, id desc;
