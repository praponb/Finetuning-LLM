with base as (
    select *
    from transactions
    where (account_id in (9, 40) and account_id = 36) or merchant in ('Shell', 'Grab')
),
agg as (
    select merchant, is_flagged, max(amount) as max_amount
    from base
    group by merchant, is_flagged
)
select merchant, is_flagged
from agg
where max_amount >= 2200
order by merchant
