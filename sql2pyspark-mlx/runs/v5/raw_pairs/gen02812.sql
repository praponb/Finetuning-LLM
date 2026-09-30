with numbered as (
    select
        id,
        status,
        paid_at,
        method,
        row_number() over (partition by status order by paid_at desc, id) as pos
    from payments
)
select
    id,
    status,
    paid_at,
    method
from numbered
where pos <= 1
