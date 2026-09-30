with matched_keys as (
    select id
    from payments
    intersect
    select id
    from payments
    where amount > 670
),
verified as (
    select s.*
    from payments s
    join matched_keys k on s.id = k.id
)
select
    method,
    amount,
    paid_at,
    status
from verified;
